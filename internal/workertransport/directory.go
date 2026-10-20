// Copyright 2026 The Agent Substrate Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package workertransport

import (
	"context"
	"fmt"
	"net"
	"sort"
	"strconv"

	"go.ate.dev/substrate/internal/substratex509"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
)

const (
	// ModeLabel is stamped by the operator/controller on network worker Pods.
	ModeLabel       = "ate.dev/worker-transport"
	NetworkMode     = "network"
	WorkerPoolLabel = "ate.dev/worker-pool"
	podUIDIndex     = "worker-pod-uid"
)

// Directory discovers endpoints from Kubernetes rather than actor requests.
// It is scoped to one node and contains only explicitly network-enabled Pods.
type Directory struct {
	index          cache.Indexer
	local          substratex509.PodIdentity
	serviceAccount string
	port           int
}

// NewDirectory builds a node-scoped index of network worker Pods. The field
// and label selectors keep it to this node's explicitly network-enabled
// workers, so an unannotated (colocated) worker never appears here and this
// package never touches the default transport. The label selector is a coarse
// pre-filter (it requires the worker-pool label to EXIST); the authoritative
// test is the usable predicate below, which also requires a non-empty value.
func NewDirectory(ctx context.Context, client kubernetes.Interface, local substratex509.PodIdentity, serviceAccount string, port int) (*Directory, error) {
	if local.NodeName == "" || local.NodeUID == "" || serviceAccount == "" || port < 1 || port > 65535 {
		return nil, fmt.Errorf("node identity, worker service account and valid port are required")
	}
	factory := informers.NewSharedInformerFactoryWithOptions(client, 0,
		informers.WithTweakListOptions(func(options *metav1.ListOptions) {
			options.FieldSelector = fields.OneTermEqualSelector("spec.nodeName", local.NodeName).String()
			options.LabelSelector = ModeLabel + "=" + NetworkMode + "," + WorkerPoolLabel
		}))
	informer := factory.Core().V1().Pods().Informer()
	if err := informer.AddIndexers(cache.Indexers{podUIDIndex: func(obj any) ([]string, error) {
		return []string{string(obj.(*corev1.Pod).UID)}, nil
	}}); err != nil {
		return nil, err
	}
	factory.Start(ctx.Done())
	if !cache.WaitForCacheSync(ctx.Done(), informer.HasSynced) {
		return nil, fmt.Errorf("network worker inventory did not synchronize")
	}
	return &Directory{informer.GetIndexer(), local, serviceAccount, port}, nil
}

// usable is the AUTHORITATIVE in-process predicate; the informer label selector
// in NewDirectory is only a coarse API-server pre-filter. They are not
// identical: the selector's "WorkerPoolLabel" term means the label EXISTS
// (including an empty value), while usable additionally requires a non-empty
// worker-pool value and a running pod with a parseable IP. The difference is
// fail-closed -- a pod the selector admits but usable rejects enters the index
// but never resolves, authorizes or lists -- so nothing usable omits can reach
// a transport. Every path that acts on a pod (pod, Resolve, Authorize, UIDs)
// goes through usable, never the raw index.
func (d *Directory) usable(pod *corev1.Pod) bool {
	return pod.UID != "" && pod.Namespace != "" && pod.Name != "" &&
		pod.Spec.NodeName == d.local.NodeName &&
		pod.Spec.ServiceAccountName == d.serviceAccount &&
		pod.Labels[ModeLabel] == NetworkMode && pod.Labels[WorkerPoolLabel] != "" &&
		pod.DeletionTimestamp == nil && pod.Status.Phase == corev1.PodRunning &&
		net.ParseIP(pod.Status.PodIP) != nil
}

func (d *Directory) pod(uid string) (*corev1.Pod, error) {
	objects, err := d.index.ByIndex(podUIDIndex, uid)
	if err != nil || len(objects) != 1 {
		return nil, fmt.Errorf("network worker %q is absent or ambiguous", uid)
	}
	pod := objects[0].(*corev1.Pod)
	if !d.usable(pod) {
		return nil, fmt.Errorf("network worker %q is not active on this node", uid)
	}
	return pod, nil
}

// Resolve returns a Pod IP and namespace; callers also pin the TLS peer's UID.
func (d *Directory) Resolve(uid string) (address, namespace string, err error) {
	pod, err := d.pod(uid)
	if err != nil {
		return "", "", err
	}
	return net.JoinHostPort(pod.Status.PodIP, strconv.Itoa(d.port)), pod.Namespace, nil
}

// Authorize rechecks, for each handshake, that the peer certificate's claimed
// Pod identity matches a live, same-node-incarnation, network-enabled worker in
// the Kubernetes inventory. A worker that was deleted or moved fails here even
// if it still holds a valid certificate.
func (d *Directory) Authorize(identity *substratex509.PodIdentity) error {
	if identity == nil || identity.NodeUID != d.local.NodeUID || identity.NodeName != d.local.NodeName {
		return fmt.Errorf("worker belongs to another node incarnation")
	}
	pod, err := d.pod(identity.PodUID)
	if err != nil {
		return err
	}
	if pod.Namespace != identity.Namespace || pod.Name != identity.PodName || pod.Spec.ServiceAccountName != identity.ServiceAccountName {
		return fmt.Errorf("worker certificate differs from Kubernetes inventory")
	}
	return nil
}

// UIDs supplies the same inventory to the stats poller without sharing its
// short-lived connections with lifecycle RPCs.
func (d *Directory) UIDs() []string {
	var uids []string
	for _, obj := range d.index.List() {
		pod := obj.(*corev1.Pod)
		if d.usable(pod) {
			uids = append(uids, string(pod.UID))
		}
	}
	sort.Strings(uids)
	return uids
}
