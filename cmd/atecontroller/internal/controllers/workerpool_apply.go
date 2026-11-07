// Copyright 2026 The Agent Substrate Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package controllers

import (
	"fmt"
	"slices"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/intstr"
	appsv1ac "k8s.io/client-go/applyconfigurations/apps/v1"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"
	metav1ac "k8s.io/client-go/applyconfigurations/meta/v1"

	atev1 "go.ate.dev/substrate/api/infra/k8s/ate/v1"
	"go.ate.dev/substrate/internal/deviceplugin"
	"go.ate.dev/substrate/internal/installdefaults"
	"go.ate.dev/substrate/internal/nodepath"
	"go.ate.dev/substrate/internal/preview"
	"go.ate.dev/substrate/internal/workercapacity"
)

// workerOTelResourceAttributes mirrors atelet.yaml; service.instance.id is the pod
// uid so each worker pod is a distinct telemetry source.
const workerOTelResourceAttributes = "k8s.namespace.name=$(POD_NAMESPACE),k8s.pod.name=$(POD_NAME),k8s.pod.uid=$(POD_UID),k8s.node.name=$(NODE_NAME),service.instance.id=$(POD_UID)"

// workerTerminationGracePeriodSeconds is the hardcoded pod termination grace
// period for worker pods (60 minutes).
const workerTerminationGracePeriodSeconds int64 = 3600

// Rollout settings for the pool's Deployment. A pool edit rolls the workers
// through the eviction path, so the strategy is chosen for the actors on them:
//
//   - maxSurge 0 with a 10% maxUnavailable, which Kubernetes raises to at
//     least one pod. To make the roll smooth, make it gradual, maxSurge should
//     be 0 to avoid any potential stall for small size WorkerPool.
//   - progressDeadlineSeconds above the 3600s pod grace period, with room for
//     the replacement to start, so a batch waiting out the drain does not have
//     kubectl rollout status report a failed rollout.
const (
	workerRolloutMaxSurge                = 0
	workerRolloutMaxUnavailable          = "10%"
	workerRolloutProgressDeadlineSeconds = int32(4800)
)

// workerOTelSettings is the telemetry configuration propagated to worker
// pods. A zero value sets only the worker's resource identity.
type workerOTelSettings struct {
	// Endpoint is the OTLP collector address the worker exports to directly, when it
	// finds no atelet relay. Empty leaves the worker exporting through the relay
	// only, and exporting nothing without one.
	Endpoint string
	// MetricExportInterval overrides the SDK's 60s PeriodicReader interval. It is
	// the raw OTEL_METRIC_EXPORT_INTERVAL value, i.e. whole milliseconds; the SDK
	// falls back to its default when it does not parse. Empty keeps the default.
	//
	// worker registers no instruments of its own, so its only telemetry is
	// otelgrpc's rpc.server.* and it stays invisible to the collector until the
	// first export tick fires. Shortening the interval is what keeps that
	// startup gap inside an e2e budget; production leaves it unset.
	MetricExportInterval string
	// MetricExportTimeout overrides the SDK's  per-export timeout, in the same
	// whole-millisecond form as MetricExportInterval. Empty keeps the default.
	MetricExportTimeout string
	// TracesSampler and TracesSamplerArg are the raw OTEL_TRACES_SAMPLER and
	// OTEL_TRACES_SAMPLER_ARG values, passed through untouched: worker's own
	// serverboot resolution validates them. Empty sampler keeps the worker's
	// default and drops the arg, which is dead config on its own.
	TracesSampler    string
	TracesSamplerArg string
	// LogsExporter, TracesExporter and MetricsExporter are the
	// OTEL_LOGS_EXPORTER, OTEL_TRACES_EXPORTER and OTEL_METRICS_EXPORTER set on
	// the worker, passed through untouched. They are worker-only values rather than
	// the controller's own: on the control plane, metrics prometheus means
	// scraped instead of pushed, and ateworker serves no /metrics. Empty keeps
	// ateworker's defaults: none for logs, otlp for traces and metrics.
	LogsExporter    string
	TracesExporter  string
	MetricsExporter string
}

// workerPoolLabel names the WorkerPool on each of its worker pods.
const workerPoolLabel = "ate.dev/worker-pool"

// workerTransportLabel marks a worker pod's transport. The node atelet's
// network-worker directory admits a worker only if its pod carries this label
// set to workerTransportNetwork; it is reserved to controllers (the
// WorkerPoolPodTemplate validation rejects ate.dev/ keys), so only this
// controller sets it, and only for a pool that opted into network transport.
const (
	workerTransportLabel   = "ate.dev/worker-transport"
	workerTransportNetwork = "network"
)

// guestUsernsInitContainerName names the one-shot initContainer a network
// worker pod runs before the worker to raise the guest's
// user.max_user_namespaces so gVisor's sandbox can create its user namespace.
// Reused as the idempotency key: a second apply removes any existing
// initContainer with this name before adding, so it is never duplicated.
const guestUsernsInitContainerName = "guest-userns"

// guestUsernsSetupArg is the worker flag that selects the binary's one-shot
// guest-userns setup mode (cmd/ateworker-gvisor/guestuserns.go). The
// initContainer runs the SAME worker image with only this arg; the worker
// container is never given it.
const guestUsernsSetupArg = "--setup-guest-userns"

// Network-worker transport install contract. These match the
// atelet's own defaults (cmd/atelet/network_worker.go: --network-worker-port
// 8086, --worker-services-listen-address :8087) and the atelet-workers
// ClusterIP Service that the deployment adds in front of atelet's
// networked WorkerSupport listener. The controller tells a network worker to
// serve on networkWorkerListenPort and to reach atelet on the atelet-workers
// Service in the install's own namespace -- the namespace is systemNamespace,
// never hard-coded, so a relocated install still points the worker at the
// right atelet. The Service name and the two ports are the fixed install
// contract shared with the atelet flags and the Service manifest.
const (
	// networkWorkerListenPort is the TCP port a network worker serves its
	// Worker and WorkerStaging services on. atelet dials it (its
	// --network-worker-port default is the same value).
	networkWorkerListenPort = 8086
	// ateletWorkersServiceName and ateletWorkersServicePort name the ClusterIP
	// Service in front of atelet's networked WorkerSupport listener
	// (--worker-services-listen-address :8087). A network worker dials
	// <service>.<systemNamespace>.svc:<port>.
	ateletWorkersServiceName = "atelet-workers"
	ateletWorkersServicePort = 8087
)

// networkWorkerArgs returns the worker flags that turn on the worker's network
// mode for a pool that opted into network transport: the local TCP listen
// address and the atelet-workers Service endpoint. The worker binds a wildcard
// listener on networkWorkerListenPort (atelet dials the worker Pod's IP; the
// pool's NetworkPolicy narrows who can connect and mTLS gates the caller,
// never the bind) and dials atelet at the Service in the install's namespace.
func networkWorkerArgs(systemNamespace string) []string {
	return []string{
		fmt.Sprintf("--worker-listen-address=0.0.0.0:%d", networkWorkerListenPort),
		fmt.Sprintf("--atelet-address=%s.%s.svc:%d", ateletWorkersServiceName, systemNamespace, ateletWorkersServicePort),
	}
}

// ateBaseVolume is the shared node-filesystem hostPath volume (mounted at
// nodepath.BasePath) a colocated worker reaches the atelet's Unix sockets and
// the sandbox-runtimes file through. A network worker runs in a separate guest
// Pod with no shared node filesystem, so applyNetworkTransport removes it.
const ateBaseVolume = "ate-base"

const (
	atunnelIdentityVolume       = "atunnel-identity"
	atunnelIdentityMountPath    = "/run/podidentity.podcert.ate.dev"
	atunnelEgressTrustVolume    = "atunnel-egress-trust"
	atunnelEgressTrustMountPath = "/run/servicedns.podcert.ate.dev"
	workerCapacityVolume        = "worker-capacity"
)

// buildDeploymentApplyConfig constructs the SSA apply configuration for the
// Deployment managed by a WorkerPool. Only fields owned by this controller
// are declared here. otel is propagated to the worker container (see
// workerContainerEnv).
func buildDeploymentApplyConfig(wp *atev1.WorkerPool, otel workerOTelSettings, systemNamespace, ateletServiceAccount, routerServiceAccount, dnsEgressPolicy string) *appsv1ac.DeploymentApplyConfiguration {
	labels := map[string]string{}
	annotations := map[string]string{}
	if wp.Spec.Template != nil {
		for key, value := range wp.Spec.Template.Labels {
			labels[key] = string(value)
		}
		for key, value := range wp.Spec.Template.Annotations {
			annotations[key] = value
		}
	}
	labels[workerPoolLabel] = wp.Name

	args := []string{
		"--pod-uid=$(POD_UID)",
		"--pod-ips=$(POD_IPS)",
		"--atunnel-listen-address=:443",
		"--atunnel-connect-listen-address=:8443",
		"--atunnel-credential-bundle=" + atunnelIdentityMountPath + "/credential-bundle.pem",
		"--atunnel-trust-bundle=" + atunnelIdentityMountPath + "/trust-bundle.pem",
		// The peers atunnel authenticates live in substrate's namespace, not
		// the worker's, so the controller passes their identities rather than
		// letting worker assume the default install. --atunnel-client-identity
		// has been accepted by every worker that carries this controller's
		// contemporaries, so it is always safe to pass.
		"--atunnel-client-identity=" + installdefaults.SPIFFEID(systemNamespace, routerServiceAccount),
	}

	// --atunnel-broker-identity is newer than the oldest worker a WorkerPool
	// may still pin, and a worker exits on an unrecognized flag. A worker
	// without the flag hardcodes the canonical identity, and a worker with it
	// defaults to the same, so omitting the flag when it carries that value is
	// equivalent for both. A relocated or renamed install passes something
	// else and needs an image new enough to accept it, which it necessarily
	// has.
	if brokerIdentity := installdefaults.SPIFFEID(systemNamespace, ateletServiceAccount); brokerIdentity != installdefaults.AteletSPIFFEID(installdefaults.SystemNamespace) {
		args = append(args, "--atunnel-broker-identity="+brokerIdentity)
	}

	args = append(args,
		"--atunnel-egress-listen-address=0.0.0.0:15001",
		"--atunnel-egress-trust-bundle="+atunnelEgressTrustMountPath+"/trust-bundle.pem",
		"--dns-egress-policy="+dnsEgressPolicy,
	)

	// worker gets preview features when the controller has them.
	enabledGates := func() []string {
		ret := []string{}
		for k, v := range preview.AsMap() {
			if v {
				ret = append(ret, k)
			}
		}
		// Map order is random; a stable arg avoids needless rollouts.
		slices.Sort(ret)
		return ret
	}
	previewArg := "--preview=" + strings.Join(enabledGates(), ",")
	args = append(args, previewArg)

	containerAC := corev1ac.Container().
		WithName("worker").
		WithImage(wp.Spec.WorkerImage).
		WithArgs(args...).
		WithPorts(corev1ac.ContainerPort().
			WithName("https").
			WithContainerPort(443).
			WithProtocol(corev1.ProtocolTCP),
			corev1ac.ContainerPort().
				WithName("connect").
				WithContainerPort(8443).
				WithProtocol(corev1.ProtocolTCP),
			corev1ac.ContainerPort().
				WithName("readyz").
				WithContainerPort(8080).
				WithProtocol(corev1.ProtocolTCP)).
		WithReadinessProbe(corev1ac.Probe().
			WithHTTPGet(corev1ac.HTTPGetAction().
				WithPath("/readyz").
				WithPort(intstr.FromString("readyz")))).
		WithSecurityContext(workerSecurityContext(wp.Spec.DefaultSandboxClass())).
		WithEnv(workerContainerEnv(otel)...).
		WithVolumeMounts(
			corev1ac.VolumeMount().
				WithName(workerCapacityVolume).
				WithMountPath(workercapacity.MountPath).
				WithReadOnly(true),
			corev1ac.VolumeMount().
				WithName(ateBaseVolume).
				WithMountPath(nodepath.BasePath).
				WithMountPropagation(corev1.MountPropagationHostToContainer),
			corev1ac.VolumeMount().
				WithName(atunnelIdentityVolume).
				WithMountPath(atunnelIdentityMountPath).
				WithReadOnly(true),
			corev1ac.VolumeMount().
				WithName(atunnelEgressTrustVolume).
				WithMountPath(atunnelEgressTrustMountPath).
				WithReadOnly(true),
		)

	podSpecAC := corev1ac.PodSpec().
		WithSecurityContext(corev1ac.PodSecurityContext().
			WithRunAsUser(0).
			WithRunAsGroup(0)).
		WithVolumes(
			corev1ac.Volume().
				WithName(workerCapacityVolume).
				WithDownwardAPI(corev1ac.DownwardAPIVolumeSource().
					WithItems(
						resourceFieldRefFile(workercapacity.CPULimitFile, "limits.cpu", milliCores),
						resourceFieldRefFile(workercapacity.MemoryLimitFile, "limits.memory", wholeBytes),
					)),
			corev1ac.Volume().
				WithName(ateBaseVolume).
				WithHostPath(corev1ac.HostPathVolumeSource().
					WithPath(nodepath.BasePath).
					WithType(corev1.HostPathDirectoryOrCreate)),
			corev1ac.Volume().
				WithName(atunnelIdentityVolume).
				WithProjected(corev1ac.ProjectedVolumeSource().
					WithSources(
						corev1ac.VolumeProjection().
							WithPodCertificate(corev1ac.PodCertificateProjection().
								WithSignerName("podidentity.podcert.ate.dev/identity").
								WithKeyType("ECDSAP256").
								WithCredentialBundlePath("credential-bundle.pem")),
						corev1ac.VolumeProjection().
							WithClusterTrustBundle(corev1ac.ClusterTrustBundleProjection().
								WithSignerName("podidentity.podcert.ate.dev/identity").
								WithLabelSelector(metav1ac.LabelSelector().
									WithMatchLabels(map[string]string{"podcert.ate.dev/canarying": "live"})).
								WithPath("trust-bundle.pem")),
					),
				),
			corev1ac.Volume().
				WithName(atunnelEgressTrustVolume).
				WithProjected(corev1ac.ProjectedVolumeSource().
					WithSources(
						corev1ac.VolumeProjection().
							WithClusterTrustBundle(corev1ac.ClusterTrustBundleProjection().
								WithSignerName("servicedns.podcert.ate.dev/identity").
								WithLabelSelector(metav1ac.LabelSelector().
									WithMatchLabels(map[string]string{"podcert.ate.dev/canarying": "live"})).
								WithPath("trust-bundle.pem")),
					),
				),
		)

	applyWorkerPoolPodTemplate(podSpecAC, containerAC, wp.Spec.Template)
	applySandboxClassToleration(podSpecAC, wp.Spec.DefaultSandboxClass())
	maybeApplyMicroVMPodShape(podSpecAC, containerAC, wp.Spec.DefaultSandboxClass())
	applyNetworkTransport(podSpecAC, containerAC, labels, wp.Spec.NetworkTransport, systemNamespace)
	podSpecAC.WithContainers(containerAC)
	podSpecAC.WithTerminationGracePeriodSeconds(workerTerminationGracePeriodSeconds)

	return appsv1ac.Deployment(wp.Name, wp.Namespace).
		WithLabels(labels).
		WithAnnotations(annotations).
		WithOwnerReferences(metav1ac.OwnerReference().
			WithAPIVersion(atev1.GroupVersion.String()).
			WithKind("WorkerPool").
			WithName(wp.Name).
			WithUID(wp.UID).
			WithController(true).
			WithBlockOwnerDeletion(true)).
		WithSpec(appsv1ac.DeploymentSpec().
			WithReplicas(wp.Spec.Replicas).
			WithStrategy(appsv1ac.DeploymentStrategy().
				WithType(appsv1.RollingUpdateDeploymentStrategyType).
				WithRollingUpdate(appsv1ac.RollingUpdateDeployment().
					WithMaxSurge(intstr.FromInt32(workerRolloutMaxSurge)).
					WithMaxUnavailable(intstr.FromString(workerRolloutMaxUnavailable)))).
			WithProgressDeadlineSeconds(workerRolloutProgressDeadlineSeconds).
			WithSelector(metav1ac.LabelSelector().
				WithMatchLabels(map[string]string{workerPoolLabel: wp.Name})).
			WithTemplate(corev1ac.PodTemplateSpec().
				WithLabels(labels).
				WithAnnotations(annotations).
				WithSpec(podSpecAC)))
}

// workerContainerEnv always sets the resource identity, since the worker
// exports through atelet's relay whenever atelet has a collector, and adds the
// OTLP endpoint only when one is configured. Every ref precedes
// OTEL_RESOURCE_ATTRIBUTES so its $(...) substitutions resolve.
func workerContainerEnv(otel workerOTelSettings) []*corev1ac.EnvVarApplyConfiguration {
	// The pool pair also labels every usage record. A worker pod runs in its
	// WorkerPool's namespace.
	envs := []*corev1ac.EnvVarApplyConfiguration{
		fieldRefEnv("POD_UID", "metadata.uid"),
		fieldRefEnv("POD_NAMESPACE", "metadata.namespace"),
		fieldRefEnv("WORKER_POOL_NAME", "metadata.labels['"+workerPoolLabel+"']"),
		fieldRefEnv("POD_NAME", "metadata.name"),
		fieldRefEnv("NODE_NAME", "spec.nodeName"),
		// Resolved when the container starts, so a worker process started in a
		// recreated pod sandbox registers the sandbox's new IPs.
		fieldRefEnv("POD_IPS", "status.podIPs"),
	}
	if otel.Endpoint != "" {
		envs = append(envs, corev1ac.EnvVar().WithName("OTEL_EXPORTER_OTLP_ENDPOINT").WithValue(otel.Endpoint))
	}
	envs = append(envs, corev1ac.EnvVar().WithName("OTEL_RESOURCE_ATTRIBUTES").WithValue(workerOTelResourceAttributes))
	if otel.MetricExportInterval != "" {
		envs = append(envs, corev1ac.EnvVar().
			WithName("OTEL_METRIC_EXPORT_INTERVAL").
			WithValue(otel.MetricExportInterval))
	}
	if otel.MetricExportTimeout != "" {
		envs = append(envs, corev1ac.EnvVar().
			WithName("OTEL_METRIC_EXPORT_TIMEOUT").
			WithValue(otel.MetricExportTimeout))
	}
	if otel.LogsExporter != "" {
		envs = append(envs, corev1ac.EnvVar().
			WithName("OTEL_LOGS_EXPORTER").
			WithValue(otel.LogsExporter))
	}
	if otel.TracesSampler != "" {
		envs = append(envs, corev1ac.EnvVar().
			WithName("OTEL_TRACES_SAMPLER").
			WithValue(otel.TracesSampler))
		if otel.TracesSamplerArg != "" {
			envs = append(envs, corev1ac.EnvVar().
				WithName("OTEL_TRACES_SAMPLER_ARG").
				WithValue(otel.TracesSamplerArg))
		}
	}
	if otel.TracesExporter != "" {
		envs = append(envs, corev1ac.EnvVar().
			WithName("OTEL_TRACES_EXPORTER").
			WithValue(otel.TracesExporter))
	}
	if otel.MetricsExporter != "" {
		envs = append(envs, corev1ac.EnvVar().
			WithName("OTEL_METRICS_EXPORTER").
			WithValue(otel.MetricsExporter))
	}
	return envs
}

// Divisors for resourceFieldRefFile. The downward API reports
// ceil(limit/divisor), so these are the units the value arrives in; the default
// divisor of one core would round a fractional CPU limit up to a whole one.
const (
	milliCores = "1m"
	wholeBytes = "1"
)

// resourceFieldRefFile projects a container resource in units of divisor.
func resourceFieldRefFile(path, resourceName, divisor string) *corev1ac.DownwardAPIVolumeFileApplyConfiguration {
	return corev1ac.DownwardAPIVolumeFile().
		WithPath(path).
		WithResourceFieldRef(corev1ac.ResourceFieldSelector().
			WithContainerName("worker").
			WithResource(resourceName).
			WithDivisor(resource.MustParse(divisor)))
}

func fieldRefEnv(name, fieldPath string) *corev1ac.EnvVarApplyConfiguration {
	return corev1ac.EnvVar().
		WithName(name).
		WithValueFrom(corev1ac.EnvVarSource().
			WithFieldRef(corev1ac.ObjectFieldSelector().
				WithFieldPath(fieldPath)))
}

// workerGvisorCapabilities is the fixed capability set for gVisor workers.
// runsc's gofer maps a full-range identity in a user namespace
// (SETUID/SETGID, plus SETFCAP: since Linux 5.12, writing a uid_map that maps
// uid 0 needs CAP_SETFCAP over the parent user namespace, and without it the
// gofer fails with "cannot create gofer process: fork/exec /proc/self/exe:
// operation not permitted"), worker mounts need SYS_ADMIN, actor networking
// needs NET_ADMIN/NET_RAW, and rootfs ownership and modes need
// DAC_OVERRIDE/FOWNER/CHOWN.
//
// The sandbox's user namespace supplies its own SYS_CHROOT (gofer) and
// SYS_PTRACE (systrap); the outer worker needs neither, nor SETPCAP. That user
// namespace comes from runsc's modifySpecForDirectfs, which creates an
// identity-mapped userns only while directfs is on and --network is not host.
// Both hold with the runsc flags in cmd/ateworker-gvisor/kodata/internal_flags.json;
// setting --directfs=false or --network=host there moves the gofer and systrap
// back into the worker's userns, where runsc only logs that it drops the
// missing capabilities. Since Linux 5.8, creating the 0:0 overlay whiteouts
// does not require MKNOD.
var workerGvisorCapabilities = []corev1.Capability{
	"NET_ADMIN", "SYS_ADMIN", "SETUID", "SETGID", "DAC_OVERRIDE",
	"FOWNER", "CHOWN", "NET_RAW", "SETFCAP",
}

// netBindServiceCapability lets a process bind a privileged port (<1024) as a
// non-root or capability-limited process. A network worker needs it to bind
// atunnel on :443 inside a guest runtime class that does not set
// net.ipv4.ip_unprivileged_port_start=0. Added only by applyNetworkTransport,
// so it never widens a colocated worker's set.
const netBindServiceCapability corev1.Capability = "NET_BIND_SERVICE"

// addWorkerCapability appends cap to the worker container's security-context
// capability Add list if it is not already present. Idempotent: applying the
// same pod twice does not duplicate the capability. It only touches the Add
// list, leaving Drop (and every other capability already present) exactly as
// workerSecurityContext set it. It is a no-op if the container has no security
// context or capabilities set, which never happens for a worker container the
// controller builds but keeps the helper total.
func addWorkerCapability(containerAC *corev1ac.ContainerApplyConfiguration, cap corev1.Capability) {
	if containerAC.SecurityContext == nil || containerAC.SecurityContext.Capabilities == nil {
		return
	}
	if slices.Contains(containerAC.SecurityContext.Capabilities.Add, cap) {
		return
	}
	containerAC.SecurityContext.Capabilities.Add = append(containerAC.SecurityContext.Capabilities.Add, cap)
}

// workerMicroVMCapabilities retains virtiofsd's eight default child
// capabilities, plus NET_ADMIN and SYS_ADMIN for worker networking and mounts.
// MKNOD and FSETID preserve guest device-node creation and setgid files.
// DAC_READ_SEARCH is not granted: virtiofsd only adds it to its child set to
// open file handles, and virtiofsdArgs in cmd/ateworker-microvm/internal/kata
// turns file handles off. Unlike gVisor, this runtime does not need NET_RAW.
//
// SETPCAP stays because virtiofsd's serving child drops its bounding set with
// PR_CAPBSET_DROP, which libcap-ng only issues while CAP_SETPCAP is effective.
// Without it the child keeps the worker's full bounding set, and a virtiofsd
// built against unpatched libcap-ng (>= 0.8.1) refuses to start. Granting it
// adds nothing reachable: the worker's permitted set already equals its
// bounding set.
//
// The hypervisor devices are not capabilities: they come from atelet's device
// plugin (see maybeApplyMicroVMPodShape).
var workerMicroVMCapabilities = []corev1.Capability{
	"NET_ADMIN", "SYS_ADMIN", "SETUID", "SETGID", "DAC_OVERRIDE",
	"FOWNER", "CHOWN", "MKNOD", "SETFCAP", "FSETID", "SETPCAP",
}

// workerSecurityContext returns the worker container security context for a sandbox
// class. Neither class runs privileged; they differ in their capability set.
// Both declare seccomp Unconfined because their sandbox child pivot_root()s,
// which the default profile denies. An empty class defaults to gVisor.
func workerSecurityContext(class atev1.SandboxClass) *corev1ac.SecurityContextApplyConfiguration {
	// Both runtimes mount inside the worker — runsc pivots root and the worker
	// remounts /sys/fs/cgroup to nest per-actor cgroups; the micro-VM worker
	// shares /run/kata-containers and remounts /proc/sys — and the default
	// AppArmor profile denies mount (enforced on GKE COS and Ubuntu, a no-op on
	// nodes that do not load AppArmor). A privileged worker got this implicitly;
	// unprivileged must request it. Replacing Unconfined with a tailored profile
	// per runtime is a follow-up.
	sc := corev1ac.SecurityContext().
		WithRunAsUser(0).
		WithRunAsGroup(0).
		WithPrivileged(false).
		WithAllowPrivilegeEscalation(false).
		WithAppArmorProfile(corev1ac.AppArmorProfile().
			WithType(corev1.AppArmorProfileTypeUnconfined))

	if class == atev1.SandboxClassMicroVM {
		// Give up the default seccomp profile so virtiofsd keeps its own sandbox,
		// which pivot_root()s — a syscall the profile denies whatever capabilities
		// the worker holds. The trade favors containing the guest: virtiofsd parses
		// requests from it, so its namespaces are part of the guest-to-host
		// boundary, while the pod-wide profile mostly confines worker, which we
		// trust. Both guest-facing processes confine themselves anyway, virtiofsd
		// with its own seccomp filter and eight capabilities, cloud-hypervisor with
		// per-thread filters.
		return sc.
			WithCapabilities(corev1ac.Capabilities().
				WithDrop("ALL").
				WithAdd(workerMicroVMCapabilities...)).
			WithSeccompProfile(corev1ac.SeccompProfile().
				WithType(corev1.SeccompProfileTypeUnconfined))
	}
	// runsc's sandbox child also pivot_root()s, so it needs the same relaxation:
	// a cluster that defaults seccomp to RuntimeDefault denies the syscall whatever
	// capabilities the worker holds, and the sandbox then dies during startup.
	// Declare Unconfined explicitly so the worker does not depend on the cluster
	// leaving the profile unset.
	return sc.
		WithCapabilities(corev1ac.Capabilities().
			WithDrop("ALL").
			WithAdd(workerGvisorCapabilities...)).
		WithSeccompProfile(corev1ac.SeccompProfile().
			WithType(corev1.SeccompProfileTypeUnconfined))
}

// maybeApplyMicroVMPodShape adds the /dev/kvm device and node placement a
// micro-VM (kata + cloud-hypervisor) worker pool needs, on top of any
// pod-template settings. No-op unless sandboxClass is the micro-VM class.
//
// TODO: this hardcodes one sandbox class's pod requirements in the controller.
// Consider making it generic so a sandbox class can declare its own pod shape
// (e.g. required devices/mounts + node placement on the SandboxConfig spec)
// instead of branching on SandboxClass here, so new classes don't need a
// controller change.
func maybeApplyMicroVMPodShape(
	podSpecAC *corev1ac.PodSpecApplyConfiguration,
	containerAC *corev1ac.ContainerApplyConfiguration,
	sandboxClass atev1.SandboxClass,
) {
	if sandboxClass != atev1.SandboxClassMicroVM {
		return
	}

	// The micro-VM runtime needs /dev/kvm to create the VM. Request it as an
	// extended resource served by atelet's device plugin rather than
	// hostPath-mounting it: a container's device access is gated by the cgroup
	// device controller, which denies /dev/kvm by default, and kubelet's device
	// manager is what adds the matching allow rule. A hostPath mount alone
	// yields EPERM unless the pod is privileged, which would hand over every
	// other device on the node too.
	//
	// atelet advertises it only on nodes where the device exists, so the
	// request also keeps the pod off nodes that cannot run a micro-VM.
	addDeviceResourceLimits(containerAC, deviceplugin.ResourceKVM)

	// The runtime also opens /dev/net/tun to build the guest's tap, but that
	// one needs no grant: it is in the runtime's default device allow-list, so
	// a bind mount is enough and creating the tap is a capability question
	// (CAP_NET_ADMIN, above) rather than a device one. That default is a
	// compatibility guarantee rather than an accident — runc dropped the rule
	// once, restored it when it broke users, and crun matched.
	//
	// Advertising tun as a resource would only add a placement constraint that
	// can fail: it is not scarce, and a node where the tun module has yet to
	// load would refuse a worker that needs nothing from it.
	containerAC.WithVolumeMounts(corev1ac.VolumeMount().
		WithName(tunDeviceVolume).
		WithMountPath(tunDevicePath))
	podSpecAC.WithVolumes(corev1ac.Volume().
		WithName(tunDeviceVolume).
		WithHostPath(corev1ac.HostPathVolumeSource().
			WithPath(tunDevicePath).
			WithType(corev1.HostPathCharDev)))

	// Placement onto KVM-capable nodes comes from the device request above: the
	// scheduler only picks nodes advertising the resource, and atelet advertises
	// it only where the device exists. That is derived from the node itself,
	// unlike an ate.dev/sandboxClass label, which is a hand-applied convention
	// that can be wrong in both directions (a labeled node without KVM takes
	// pods that then fail at runtime; an unlabeled KVM node is invisible
	// capacity). Note this makes the pool depend on atelet having registered:
	// with no node advertising the resource, workers stay Pending rather than
	// landing somewhere they cannot run.
	//
	// Extended resources constrain where this pod fits but repel nothing, so a
	// cluster reserving nested-virt nodes with a taint also needs the toleration
	// that applySandboxClassToleration adds for every class.
}

// sandboxClassTaintKey is the taint key a cluster puts on a node pool reserved
// for one sandbox class, with the class name as the value:
// ate.dev/sandboxClass=<class>:NoSchedule. The atelet DaemonSet tolerates the
// key for any value.
const sandboxClassTaintKey = "ate.dev/sandboxClass"

// applySandboxClassToleration lets worker pods schedule onto a node pool
// tainted for the pool's own sandbox class. Without it a cluster that reserves
// nodes per class with the ate.dev/sandboxClass taint leaves every worker
// Pending. The toleration is additive on top of the WorkerPool's configurable
// spec.template tolerations. An empty class means the API default, gvisor,
// which is what the CRD's defaulting produces on the server.
func applySandboxClassToleration(
	podSpecAC *corev1ac.PodSpecApplyConfiguration,
	sandboxClass atev1.SandboxClass,
) {
	if sandboxClass == "" {
		sandboxClass = atev1.SandboxClassGvisor
	}
	podSpecAC.WithTolerations(corev1ac.Toleration().
		WithKey(sandboxClassTaintKey).
		WithOperator(corev1.TolerationOpEqual).
		WithValue(string(sandboxClass)).
		WithEffect(corev1.TaintEffectNoSchedule))
}

// The tun device node a micro-VM worker bind-mounts to build the guest's tap.
const (
	tunDeviceVolume = "dev-net-tun"
	tunDevicePath   = "/dev/net/tun"
)

// addDeviceResourceLimits requests one unit of each named extended resource,
// merging into whatever limits the pod template already set.
func addDeviceResourceLimits(containerAC *corev1ac.ContainerApplyConfiguration, resourceNames ...string) {
	if containerAC.Resources == nil {
		containerAC.WithResources(corev1ac.ResourceRequirements())
	}
	limits := corev1.ResourceList{}
	if containerAC.Resources.Limits != nil {
		limits = *containerAC.Resources.Limits
	}
	for _, name := range resourceNames {
		limits[corev1.ResourceName(name)] = resource.MustParse("1")
	}
	containerAC.Resources.WithLimits(limits)
}

// applyNetworkTransport shapes a worker pod for network transport: it runs in a
// separate guest Pod reached over authenticated Pod networking instead of the
// node's shared filesystem. No-op when nt is nil, so a colocated pool's pod is
// byte-for-byte unchanged.
//
// When nt is set it:
//   - gives the pod the configured RuntimeClass, so it can be placed on an
//     isolating guest runtime class;
//   - adds the reserved ate.dev/worker-transport=network label the node atelet's
//     network-worker directory authorizes a network worker by (into labels,
//     which the caller puts on the Deployment and the pod template but NOT the
//     immutable selector);
//   - replaces the shared node-filesystem hostPath volume and its mount, which
//     a separate guest cannot and must not use, with a writable emptyDir at the
//     same path so the worker has a real (non-overlayfs) filesystem to MkdirAll
//     its subtree and mount per-container overlay rootfs upperdirs under.
//
// The service account is left as applyWorkerPoolPodTemplate set it (the install
// default "ateworker", or the pool's own ServiceAccountName): network transport
// does not change the worker's identity. atelet, however, admits a network
// worker only if it runs as the one account its --network-worker-service-account
// flag names (default "ateworker"), so a network pool whose template sets
// another serviceAccountName is not rejected here but its workers cannot
// authenticate to atelet.
//
// It passes the worker-side network flags (--worker-listen-address and
// --atelet-address) so the worker binary serves its lifecycle and staging
// services over TCP mTLS and dials WorkerSupport over the atelet-workers
// Service. These flags are only ever passed to a pool that opted in, so a
// colocated worker never receives a flag it does not parse.
func applyNetworkTransport(
	podSpecAC *corev1ac.PodSpecApplyConfiguration,
	containerAC *corev1ac.ContainerApplyConfiguration,
	labels map[string]string,
	nt *atev1.WorkerPoolNetworkTransport,
	systemNamespace string,
) {
	if nt == nil {
		return
	}

	podSpecAC.WithRuntimeClassName(nt.RuntimeClassName)

	// Turn on the worker's network mode. WithArgs appends, so these follow the
	// base args already set on the container. Only a network pool reaches here,
	// so a colocated worker never sees these flags.
	containerAC.WithArgs(networkWorkerArgs(systemNamespace)...)

	// A network worker binds atunnel on :443, a privileged port. A colocated
	// worker relies on the node's containerd setting net.ipv4.ip_unprivileged_port_start=0 for
	// the pod (enable_unprivileged_ports), so uid 0 binds :443 without a
	// capability. An isolating guest runtime class does not honor that
	// containerd default, so inside the guest even uid 0 is refused with
	// "listen tcp :443: bind: permission denied" and the worker crash-loops.
	// Grant NET_BIND_SERVICE so the privileged bind is permitted regardless of
	// the sysctl. Only a network pool reaches here, so a colocated worker's
	// capability set is unchanged. Idempotent: never add a duplicate, and leave
	// drop:[ALL] and every other capability exactly as workerSecurityContext set
	// them.
	addWorkerCapability(containerAC, netBindServiceCapability)

	// Reserved to controllers; this is the only writer, and only for a pool
	// that opted in. It identifies a network worker to the node directory.
	labels[workerTransportLabel] = workerTransportNetwork

	// A separate guest shares no node filesystem: the shared node hostPath
	// cannot and must not be reached. But the worker still needs a WRITABLE
	// directory at nodepath.BasePath that is NOT the container's own overlayfs:
	// it MkdirAlls its subtree there (workers/, actors/) and, crucially, mounts
	// a per-container overlay rootfs with its upperdir under
	// <BasePath>/actors/<uid>/bundles/<c>/upper
	// (internal/imagecache/bundle_linux.go). The failure to avoid is precisely
	// that upperdir landing on the container's OWN overlayfs root -- the kernel
	// refuses an overlay whose upperdir sits on another overlayfs -- so the
	// pause-container rootfs mount failed with
	// "filesystem on ... not supported as upperdir".
	//
	// Replace the hostPath with an emptyDir of medium Memory, mounted at the
	// same path: a tmpfs inside the guest. A default-medium emptyDir is no
	// fit: Kata (3.32, Cloud Hypervisor, on EKS) places it on the guest's
	// virtio-fs share, and overlayfs refuses that as an upperdir ("upper fs
	// missing required features"), while it accepts the guest tmpfs. The
	// tmpfs is guest memory, so runtime assets, image layers and actor state
	// count against the worker's memory; size the pool's memory for them. No
	// sizeLimit: a small cap would evict them mid-actor. The worker runs as uid 0
	// (podSecurityContext RunAsUser/RunAsGroup 0, set in the base builder), so
	// it can write the emptyDir root (0777, root) and MkdirAll every subdirectory
	// it needs -- the same empty starting point a fresh node's
	// HostPathDirectoryOrCreate would give a colocated worker.
	//
	// Done as remove-then-add so a second apply over the same config does not
	// duplicate the volume or the mount: DeleteFunc strips any existing ate-base
	// (hostPath on the first pass, emptyDir on a second), then exactly one
	// emptyDir volume and one mount are appended. Only a network pool reaches
	// here, so a colocated worker keeps its hostPath untouched.
	podSpecAC.Volumes = slices.DeleteFunc(podSpecAC.Volumes, func(v corev1ac.VolumeApplyConfiguration) bool {
		return v.Name != nil && *v.Name == ateBaseVolume
	})
	containerAC.VolumeMounts = slices.DeleteFunc(containerAC.VolumeMounts, func(m corev1ac.VolumeMountApplyConfiguration) bool {
		return m.Name != nil && *m.Name == ateBaseVolume
	})
	podSpecAC.WithVolumes(
		corev1ac.Volume().
			WithName(ateBaseVolume).
			WithEmptyDir(corev1ac.EmptyDirVolumeSource().WithMedium(corev1.StorageMediumMemory)),
	)
	containerAC.WithVolumeMounts(
		corev1ac.VolumeMount().
			WithName(ateBaseVolume).
			WithMountPath(nodepath.BasePath),
	)

	// A network worker runs in an isolating guest, and some guest kernels ship
	// with user.max_user_namespaces=0. gVisor's sentry is started in a NEW user
	// namespace for sandbox networking, so such a guest refuses the clone and the
	// sandbox dies at startup. Prepend a one-shot initContainer that raises the
	// sysctl inside the pod's own guest before the worker starts. The worker
	// container's own capabilities and security context are left exactly as
	// workerSecurityContext (+ NET_BIND_SERVICE above) set them: SYS_RESOURCE is
	// added ONLY to this short-lived initContainer, never to the worker.
	applyGuestUsernsInitContainer(podSpecAC, containerAC)
}

// applyGuestUsernsInitContainer prepends (idempotently) the one-shot
// guest-userns initContainer to a network worker's pod. It runs the SAME image
// as the worker container (read from containerAC at this point, after the base
// builder set it from wp.Spec.WorkerImage) so the one-shot code and the worker
// are the same binary, and args are just the one-shot flag. Its security
// context drops ALL capabilities and adds SYS_ADMIN (to remount the guest's
// read-only /proc/sys) and SYS_RESOURCE (which CAP_SYS_RESOURCE in the user
// namespace lets the kernel honor for user.* sysctls) ONLY -- nothing else, and
// never privileged. It matches the worker's seccomp/AppArmor Unconfined because
// the remount trips the default AppArmor profile exactly as the worker's own
// mounts do (see workerSecurityContext); without that match the remount would be
// denied on a node that loads AppArmor.
//
// Idempotent, as the volume handling is: a second apply over the same
// config first removes any existing initContainer with this name, then appends
// exactly one, so re-applying never duplicates it.
func applyGuestUsernsInitContainer(
	podSpecAC *corev1ac.PodSpecApplyConfiguration,
	containerAC *corev1ac.ContainerApplyConfiguration,
) {
	// Use the worker container's own image so the one-shot is the same binary.
	// The base builder sets it (WithImage(wp.Spec.WorkerImage)) before
	// applyNetworkTransport runs, so it is always present here. A nil image means
	// the builder was reordered and this helper now runs before the image is
	// set: fail fast rather than silently emit an empty image (which would
	// produce a pod the API rejects, far from the cause). This is an internal
	// invariant of the builder, not operator input, so a panic is the right
	// signal -- it is unreachable in a correctly ordered builder and a test
	// (TestGuestUsernsInitContainerPresentOnNetworkPod) exercises the real order.
	if containerAC.Image == nil {
		panic("applyGuestUsernsInitContainer: worker container image is not set; it must run after the base builder sets WithImage")
	}
	image := *containerAC.Image

	init := corev1ac.Container().
		WithName(guestUsernsInitContainerName).
		WithImage(image).
		WithArgs(guestUsernsSetupArg).
		WithResources(corev1ac.ResourceRequirements().
			WithRequests(corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("10m"),
				corev1.ResourceMemory: resource.MustParse("16Mi"),
			})).
		WithSecurityContext(corev1ac.SecurityContext().
			WithRunAsUser(0).
			WithRunAsGroup(0).
			WithPrivileged(false).
			WithCapabilities(corev1ac.Capabilities().
				WithDrop("ALL").
				WithAdd("SYS_ADMIN", "SYS_RESOURCE")).
			WithAppArmorProfile(corev1ac.AppArmorProfile().
				WithType(corev1.AppArmorProfileTypeUnconfined)).
			WithSeccompProfile(corev1ac.SeccompProfile().
				WithType(corev1.SeccompProfileTypeUnconfined)))

	podSpecAC.InitContainers = slices.DeleteFunc(podSpecAC.InitContainers, func(c corev1ac.ContainerApplyConfiguration) bool {
		return c.Name != nil && *c.Name == guestUsernsInitContainerName
	})
	podSpecAC.WithInitContainers(init)
}

func applyWorkerPoolPodTemplate(
	podSpecAC *corev1ac.PodSpecApplyConfiguration,
	containerAC *corev1ac.ContainerApplyConfiguration,
	tmpl *atev1.WorkerPoolPodTemplate,
) {
	podSpecAC.NodeSelector = map[string]string{}
	podSpecAC.Tolerations = []corev1ac.TolerationApplyConfiguration{}
	podSpecAC.WithPriorityClassName("")
	podSpecAC.WithServiceAccountName(installdefaults.WorkerServiceAccount)
	podSpecAC.WithAffinity(corev1ac.Affinity())
	resourcesAC := corev1ac.ResourceRequirements()
	containerAC.WithResources(resourcesAC)

	if tmpl == nil {
		return
	}

	if tmpl.NodeSelector != nil {
		podSpecAC.WithNodeSelector(tmpl.NodeSelector)
	}
	podSpecAC.Tolerations = tolerationApplyValues(tolerationsToApply(tmpl.Tolerations))
	podSpecAC.WithPriorityClassName(tmpl.PriorityClassName)
	if tmpl.ServiceAccountName != nil {
		podSpecAC.WithServiceAccountName(*tmpl.ServiceAccountName)
	}

	if tmpl.NodeAffinity != nil {
		podSpecAC.WithAffinity(corev1ac.Affinity().WithNodeAffinity(nodeAffinityToApply(tmpl.NodeAffinity)))
	}

	if tmpl.Resources != nil {
		if tmpl.Resources.Requests != nil {
			resourcesAC.WithRequests(tmpl.Resources.Requests)
		}
		if tmpl.Resources.Limits != nil {
			resourcesAC.WithLimits(tmpl.Resources.Limits)
		}
	}
}

func tolerationApplyValues(tolerations []*corev1ac.TolerationApplyConfiguration) []corev1ac.TolerationApplyConfiguration {
	out := make([]corev1ac.TolerationApplyConfiguration, 0, len(tolerations))
	for _, toleration := range tolerations {
		out = append(out, *toleration)
	}
	return out
}

func tolerationsToApply(tolerations []corev1.Toleration) []*corev1ac.TolerationApplyConfiguration {
	out := make([]*corev1ac.TolerationApplyConfiguration, 0, len(tolerations))
	for i := range tolerations {
		t := &tolerations[i]
		ac := corev1ac.Toleration()
		if t.Key != "" {
			ac.WithKey(t.Key)
		}
		if t.Operator != "" {
			ac.WithOperator(t.Operator)
		}
		if t.Value != "" {
			ac.WithValue(t.Value)
		}
		if t.Effect != "" {
			ac.WithEffect(t.Effect)
		}
		if t.TolerationSeconds != nil {
			ac.WithTolerationSeconds(*t.TolerationSeconds)
		}
		out = append(out, ac)
	}
	return out
}

func nodeAffinityToApply(na *corev1.NodeAffinity) *corev1ac.NodeAffinityApplyConfiguration {
	ac := corev1ac.NodeAffinity()
	if na.RequiredDuringSchedulingIgnoredDuringExecution != nil {
		ac.WithRequiredDuringSchedulingIgnoredDuringExecution(nodeSelectorToApply(na.RequiredDuringSchedulingIgnoredDuringExecution))
	}
	for i := range na.PreferredDuringSchedulingIgnoredDuringExecution {
		term := &na.PreferredDuringSchedulingIgnoredDuringExecution[i]
		ac.WithPreferredDuringSchedulingIgnoredDuringExecution(preferredSchedulingTermToApply(term))
	}
	return ac
}

func nodeSelectorToApply(ns *corev1.NodeSelector) *corev1ac.NodeSelectorApplyConfiguration {
	ac := corev1ac.NodeSelector()
	for i := range ns.NodeSelectorTerms {
		ac.WithNodeSelectorTerms(nodeSelectorTermToApply(&ns.NodeSelectorTerms[i]))
	}
	return ac
}

func preferredSchedulingTermToApply(term *corev1.PreferredSchedulingTerm) *corev1ac.PreferredSchedulingTermApplyConfiguration {
	return corev1ac.PreferredSchedulingTerm().
		WithWeight(term.Weight).
		WithPreference(nodeSelectorTermToApply(&term.Preference))
}

func nodeSelectorTermToApply(term *corev1.NodeSelectorTerm) *corev1ac.NodeSelectorTermApplyConfiguration {
	ac := corev1ac.NodeSelectorTerm()
	for i := range term.MatchExpressions {
		ac.WithMatchExpressions(nodeSelectorRequirementToApply(&term.MatchExpressions[i]))
	}
	for i := range term.MatchFields {
		ac.WithMatchFields(nodeSelectorRequirementToApply(&term.MatchFields[i]))
	}
	return ac
}

func nodeSelectorRequirementToApply(req *corev1.NodeSelectorRequirement) *corev1ac.NodeSelectorRequirementApplyConfiguration {
	ac := corev1ac.NodeSelectorRequirement().WithKey(req.Key).WithOperator(req.Operator)
	if len(req.Values) > 0 {
		ac.WithValues(req.Values...)
	}
	return ac
}
