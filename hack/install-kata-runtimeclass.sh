#!/usr/bin/env bash

# Copyright 2026 The Agent Substrate Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Install (or delete) the `kata` RuntimeClass the e2e network lane's worker Pods
# run under. This is opt-in on top of hack/create-kind-cluster.sh, and only the
# network lane (E2E_WORKER_TRANSPORT=network) needs it.
#
# A WorkerPool with spec.networkTransport puts its workers in their own guest
# Pods, which Kubernetes places through a RuntimeClass. Nothing in a stock kind
# cluster provides one, so this script installs Kata Containers' containerd shim
# on every node and registers the handler the RuntimeClass names.
#
# This needs Kata's classic Go shim, which honours containerd's ConfigPath
# option and ships configuration-clh.toml. The Rust runtime-rs shim does
# neither. 3.x releases ship the Go shim in kata-static; from 4.1 kata-static
# carries runtime-rs and the Go shim moved to a separate kata-go-static
# tarball. The pin below is a 3.x kata-static release.
#
# Cloud Hypervisor is the hypervisor because ateworker-microvm already uses it
# (hack/microvm-assets/assemble.sh), so one VMM covers both paths.
#
# The guest runs the gVisor sandbox class, which needs no /dev/kvm of its own.
# Booting the guest does, on the node, so this script requires the same KVM that
# hack/create-kind-cluster.sh probes for.
#
# Env:
#   KIND_CLUSTER_NAME  kind cluster to install into (default: kind).
#   KUBECTL_CONTEXT    (optional) kube context; defaults to kind-$KIND_CLUSTER_NAME.
#   ARCH               target arch (default: from the nodes' own arch).
#   CACHE              tarball download dir (default: $PWD/bin/kata, gitignored).

set -o errexit -o nounset -o pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "${ROOT}"

KIND_CLUSTER_NAME="${KIND_CLUSTER_NAME:-kind}"
KUBECTL_CONTEXT="${KUBECTL_CONTEXT:-kind-${KIND_CLUSTER_NAME}}"
CACHE="${CACHE:-${ROOT}/bin/kata}"

# The RuntimeClass name, the containerd handler it resolves to, and the shim
# binary that handler loads. e2e.NetworkRuntimeClass must match RUNTIME_CLASS.
RUNTIME_CLASS="kata"
# containerd turns runtime_type io.containerd.kata.v2 into the binary
# containerd-shim-kata-v2 on its PATH.
RUNTIME_TYPE="io.containerd.kata.v2"
SHIM_BINARY="containerd-shim-kata-v2"
# The Kata install prefix. configuration-clh.toml refers to this path
# throughout, so keeping the upstream default means no rewriting.
PREFIX="/opt/kata"

KATA_VERSION="3.32.0"

usage() {
  cat <<EOF
Usage: $0 (--install | --delete)

Options:
  --install   Install the Kata shim on every node and apply the
              ${RUNTIME_CLASS} RuntimeClass.
  --delete    Remove the ${RUNTIME_CLASS} RuntimeClass from the cluster.
              (The node-side install is left in place; it is inert without it.)
  -h, --help  Show this message.
EOF
}

action=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --install) action="install" ;;
    --delete)  action="delete"  ;;
    -h|--help) usage; exit 0    ;;
    *) echo "Error: unknown argument $1" >&2; usage; exit 1 ;;
  esac
  shift
done

if [[ -z "${action}" ]]; then
  usage
  exit 1
fi

COLOR_CYAN='\033[1;36m'
COLOR_RESET='\033[0m'
log() {
  echo -e "${COLOR_CYAN}[install-kata-runtimeclass]: $*${COLOR_RESET}"
}

run_kubectl() {
  kubectl --context="${KUBECTL_CONTEXT}" "$@"
}

if [[ "${action}" == "delete" ]]; then
  log "Deleting the ${RUNTIME_CLASS} RuntimeClass..."
  run_kubectl delete --ignore-not-found runtimeclass "${RUNTIME_CLASS}"
  log "Done. (The node-side install under ${PREFIX} is left in place.)"
  exit 0
fi

# --- install ----------------------------------------------------------------

nodes=()
while IFS= read -r node; do
  nodes+=("${node}")
done < <("${ROOT}"/hack/kind.sh get nodes --name "${KIND_CLUSTER_NAME}")
if [[ "${#nodes[@]}" -eq 0 ]]; then
  echo "Error: kind cluster '${KIND_CLUSTER_NAME}' has no nodes." >&2
  exit 1
fi

# Booting a guest needs KVM on the node. hack/create-kind-cluster.sh only
# bind-mounts /dev/kvm when the Docker environment had it, so a cluster created
# without it cannot run this lane at all. Fail here rather than let every
# worker Pod fail to start much later.
for node in "${nodes[@]}"; do
  if ! docker exec "${node}" test -e /dev/kvm; then
    echo "Error: node ${node} has no /dev/kvm, so it cannot boot a RuntimeClass guest." >&2
    echo "       hack/create-kind-cluster.sh bind-mounts /dev/kvm only when the Docker" >&2
    echo "       environment exposes it. Enable nested virtualization there and recreate" >&2
    echo "       the cluster." >&2
    exit 1
  fi
done

# kind's default caps inotify instances low enough that a handful of guests
# exhaust it, which surfaces as "Creating watcher returned error too many open
# files" from the shim. The limit belongs to the kernel the nodes share with
# their Docker host, so warn rather than change host-wide state.
instances="$(docker exec "${nodes[0]}" cat /proc/sys/fs/inotify/max_user_instances)"
if [[ "${instances}" -lt 1024 ]]; then
  echo "warning: fs.inotify.max_user_instances=${instances} on the nodes' kernel; guests will" >&2
  echo "         fail to start with 'too many open files' once a few are running. Raise it on" >&2
  echo "         the Docker host (sysctl -w fs.inotify.max_user_instances=1024, as root)." >&2
fi

if [[ -z "${ARCH:-}" ]]; then
  # Match the nodes rather than this host: on macOS the Docker VM and the host
  # agree today, but the nodes are what runs the shim.
  ARCH="$(docker exec "${nodes[0]}" dpkg --print-architecture)"
fi
# Upstream publishes no checksum file, so these are the sha256 of the release
# tarballs, taken by downloading them. Bump both together.
case "${ARCH}" in
  amd64) want_sha="1449ecea50bd91fa73a94648db195d18950fe869ba4b1f12d05f55f1fa7c1b01" ;;
  arm64) want_sha="8736c054d9223974735394f822000823baef509e1c33405ec798240fa9b6e4b5" ;;
  *) echo "Error: unsupported ARCH '${ARCH}' (want amd64 or arm64)." >&2; exit 1 ;;
esac

tarball="${CACHE}/kata-static-${KATA_VERSION}-${ARCH}.tar.zst"
url="https://github.com/kata-containers/kata-containers/releases/download/${KATA_VERSION}/kata-static-${KATA_VERSION}-${ARCH}.tar.zst"

mkdir -p "${CACHE}"

verify_sha() {
  local file="$1" want="$2" got
  got="$(sha256sum "${file}" 2>/dev/null | cut -d' ' -f1)" ||
    got="$(shasum -a 256 "${file}" | cut -d' ' -f1)"
  [[ "${got}" == "${want}" ]]
}

if [[ -f "${tarball}" ]] && verify_sha "${tarball}" "${want_sha}"; then
  log "Using cached ${tarball}."
else
  log "Downloading Kata ${KATA_VERSION} (${ARCH}) into ${CACHE}..."
  curl -fsSL -o "${tarball}.tmp" "${url}"
  if ! verify_sha "${tarball}.tmp" "${want_sha}"; then
    rm -f "${tarball}.tmp"
    echo "Error: sha256 mismatch for ${url}; expected ${want_sha}." >&2
    exit 1
  fi
  mv "${tarball}.tmp" "${tarball}"
fi

# Unpack once on this host and copy the result to every node, so a multi-node
# cluster downloads and extracts a ~700MB tarball only once. The tarball holds
# the whole prefix under opt/kata, including the kernel and rootfs image the
# guest boots and the symlinks pointing at them.
staging="$(mktemp -d)"
trap 'rm -rf "${staging}"' EXIT
log "Extracting the Kata prefix..."
tar --use-compress-program=unzstd -xf "${tarball}" -C "${staging}" opt/kata

config="${staging}/opt/kata/share/defaults/kata-containers/configuration-clh.toml"
if [[ ! -f "${config}" ]]; then
  echo "Error: ${config#"${staging}"/} is missing from the tarball. A 4.1+ kata-static" >&2
  echo "       tarball carries only the runtime-rs shim, which has no cloud-hypervisor" >&2
  echo "       configuration; this script needs the Go shim (a 3.x kata-static release)." >&2
  exit 1
fi
# The classic shim reads whichever file containerd's ConfigPath names, so point
# the default config at the cloud-hypervisor one rather than patch either.
cp "${config}" "${staging}/opt/kata/share/defaults/kata-containers/configuration.toml"

for node in "${nodes[@]}"; do
  log "Installing Kata on ${node}..."
  docker exec "${node}" mkdir -p "${PREFIX}"
  # Copy the directory's contents so symlinks travel with their targets.
  docker cp "${staging}/opt/kata/." "${node}:${PREFIX}/"
  docker exec "${node}" ln -sf "${PREFIX}/bin/${SHIM_BINARY}" "/usr/local/bin/${SHIM_BINARY}"

  # Register the handler unless it is already there: this script is safe to
  # re-run, and appending twice would leave containerd with a duplicate table
  # key and fail its config parse. Match the exact table header line, so a
  # commented-out or differently named entry does not count.
  header="[plugins.\"io.containerd.grpc.v1.cri\".containerd.runtimes.${RUNTIME_CLASS}]"
  if docker exec "${node}" grep -qxF "${header}" /etc/containerd/config.toml; then
    log "  containerd's config already has the ${RUNTIME_CLASS} handler on ${node}."
  else
    docker exec -i "${node}" tee -a /etc/containerd/config.toml >/dev/null <<EOF

${header}
  runtime_type = "${RUNTIME_TYPE}"
  [plugins."io.containerd.grpc.v1.cri".containerd.runtimes.${RUNTIME_CLASS}.options]
    ConfigPath = "${PREFIX}/share/defaults/kata-containers/configuration-clh.toml"
EOF
  fi
  # Restart containerd unless the running CRI already serves the handler, so a
  # re-run also repairs an earlier run that appended the config but stopped
  # before the restart. grep reads the whole stream (no -q), so with pipefail
  # an early grep exit cannot SIGPIPE crictl and force a needless restart.
  if docker exec "${node}" crictl info 2>/dev/null | grep -F "\"${RUNTIME_CLASS}\": {" >/dev/null; then
    log "  containerd already serves the ${RUNTIME_CLASS} handler on ${node}."
  else
    docker exec "${node}" systemctl restart containerd
  fi
done

log "Applying the ${RUNTIME_CLASS} RuntimeClass..."
run_kubectl apply -f - <<EOF
apiVersion: node.k8s.io/v1
kind: RuntimeClass
metadata:
  name: ${RUNTIME_CLASS}
handler: ${RUNTIME_CLASS}
EOF

log "Done. Run the network lane with E2E_WORKER_TRANSPORT=network."
