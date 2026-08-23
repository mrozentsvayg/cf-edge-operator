# Image URL to use all building/pushing image targets
IMG ?= controller:latest

# Get the currently used golang install path (in GOPATH/bin, unless GOBIN is set)
ifeq (,$(shell go env GOBIN))
GOBIN=$(shell go env GOPATH)/bin
else
GOBIN=$(shell go env GOBIN)
endif

# CONTAINER_TOOL defines the container tool to be used for building images.
# Be aware that the target commands are only tested with Docker which is
# scaffolded by default. However, you might want to replace it to use other
# tools. (i.e. podman)
CONTAINER_TOOL ?= docker

# Setting SHELL to bash allows bash commands to be executed by recipes.
# Options are set to exit when a recipe line exits non-zero or a piped command fails.
SHELL = /usr/bin/env bash -o pipefail
.SHELLFLAGS = -ec

.PHONY: all
all: build

##@ General

# The help target prints out all targets with their descriptions organized
# beneath their categories. The categories are represented by '##@' and the
# target descriptions by '##'. The awk command is responsible for reading the
# entire set of makefiles included in this invocation, looking for lines of the
# file as xyz: ## something, and then pretty-format the target and help. Then,
# if there's a line with ##@ something, that gets pretty-printed as a category.
# More info on the usage of ANSI control characters for terminal formatting:
# https://en.wikipedia.org/wiki/ANSI_escape_code#SGR_parameters
# More info on the awk command:
# http://linuxcommand.org/lc3_adv_awk.php

.PHONY: help
help: ## Display this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

##@ Development

.PHONY: manifests
manifests: controller-gen ## Generate WebhookConfiguration, ClusterRole and CustomResourceDefinition objects.
	"$(CONTROLLER_GEN)" rbac:roleName=manager-role crd webhook paths="./..." output:crd:artifacts:config=config/crd/bases

.PHONY: generate
generate: controller-gen ## Generate code containing DeepCopy, DeepCopyInto, and DeepCopyObject method implementations.
	"$(CONTROLLER_GEN)" object:headerFile="hack/boilerplate.go.txt" paths="./..."

.PHONY: fmt
fmt: ## Run go fmt against code.
	go fmt ./...

.PHONY: vet
vet: ## Run go vet against code.
	go vet ./...

.PHONY: test
test: manifests generate fmt vet setup-envtest ## Run tests.
	KUBEBUILDER_ASSETS="$(shell "$(ENVTEST)" use $(ENVTEST_K8S_VERSION) --bin-dir "$(LOCALBIN)" -p path)" go test $$(go list ./... | grep -v /e2e) -coverprofile cover.out

# TODO(user): To use a different vendor for e2e tests, modify the setup under 'tests/e2e'.
# The default setup assumes Kind is pre-installed and builds/loads the Manager Docker image locally.
# CertManager is installed by default; skip with:
# - CERT_MANAGER_INSTALL_SKIP=true
KIND_CLUSTER ?= cf-edge-operator-test-e2e

.PHONY: setup-test-e2e
setup-test-e2e: ## Set up a Kind cluster for e2e tests if it does not exist
	@command -v $(KIND) >/dev/null 2>&1 || { \
		echo "Kind is not installed. Please install Kind manually."; \
		exit 1; \
	}
	@case "$$($(KIND) get clusters)" in \
		*"$(KIND_CLUSTER)"*) \
			echo "Kind cluster '$(KIND_CLUSTER)' already exists. Skipping creation." ;; \
		*) \
			echo "Creating Kind cluster '$(KIND_CLUSTER)'..."; \
			$(KIND) create cluster --name $(KIND_CLUSTER) ;; \
	esac

.PHONY: test-e2e
test-e2e: setup-test-e2e manifests generate fmt vet ## Run the e2e tests. Expected an isolated environment using Kind.
	KIND=$(KIND) KIND_CLUSTER=$(KIND_CLUSTER) go test -tags=e2e ./test/e2e/ -v -ginkgo.v
	$(MAKE) cleanup-test-e2e

.PHONY: cleanup-test-e2e
cleanup-test-e2e: ## Tear down the Kind cluster used for e2e tests
	@$(KIND) delete cluster --name $(KIND_CLUSTER)

.PHONY: cover
cover: test ## Run tests and open HTML coverage report.
	go tool cover -html=cover.out

.PHONY: lint
lint: golangci-lint lint-actions lint-shell ## Run all linters (Go, GitHub Actions workflows, shell).
	"$(GOLANGCI_LINT)" run

.PHONY: lint-actions
lint-actions: actionlint shellcheck ## Lint GitHub Actions workflows (incl. embedded run: scripts via the pinned shellcheck).
	"$(ACTIONLINT)" -color -shellcheck "$(SHELLCHECK)"

.PHONY: lint-shell
lint-shell: shellcheck ## Shellcheck the repo's shell scripts.
	"$(SHELLCHECK)" $(shell find . -name '*.sh' -not -path './bin/*')

.PHONY: lint-fix
lint-fix: golangci-lint ## Run golangci-lint linter and perform fixes
	"$(GOLANGCI_LINT)" run --fix

.PHONY: lint-config
lint-config: golangci-lint ## Verify golangci-lint linter configuration
	"$(GOLANGCI_LINT)" config verify

.PHONY: helm-validate
helm-validate: ## Lint and template-render Helm chart with all optional features enabled
	helm lint charts/cf-edge-operator/
	helm template test charts/cf-edge-operator/ \
		--set features.loadBalancing.enabled=true \
		--set podDisruptionBudget.enabled=true \
		--set serviceMonitor.enabled=true \
		--set serviceMonitor.namespace=monitoring \
		--set prometheusRule.enabled=true \
		--set prometheusRule.namespace=monitoring \
		> /dev/null
	@echo "Helm chart validation passed"

.PHONY: helm-verify
helm-verify: ## Full Helm CI parity: lint + render + flag/CRD-lifecycle assertions + CRD diff (local == CI; run by .github/workflows/helm.yml).
	bash test/helm-verify.sh

.PHONY: helm-crd-sync
helm-crd-sync: manifests ## Copy generated CRDs into the chart's crds-render/ data dir.
	@rm -f charts/cf-edge-operator/crds-render/*.yaml
	@cp config/crd/bases/*.yaml charts/cf-edge-operator/crds-render/
	@echo "Synced config/crd/bases/*.yaml -> charts/cf-edge-operator/crds-render/"

.PHONY: helm-crd-diff
helm-crd-diff: ## Verify chart CRDs match generated CRDs
	@# All CRDs are chart-rendered from crds-render/ (a plain data dir loaded via
	@# .Files.Glob and emitted per-feature by templates/crds.yaml). The wrapper emits
	@# each schema body VERBATIM (crds.keep only splices annotations under metadata),
	@# so the data files must byte-match the generated bases.
	@diff -rq config/crd/bases/ charts/cf-edge-operator/crds-render/ || \
		{ echo "ERROR: Chart CRDs differ from generated CRDs. Run 'make helm-crd-sync' to copy config/crd/bases/*.yaml into charts/cf-edge-operator/crds-render/."; exit 1; }
	@echo "CRD files are in sync"

##@ Build

.PHONY: build
build: manifests generate fmt vet ## Build manager binary.
	go build -o bin/cf-edge-operator cmd/main.go

.PHONY: run
run: manifests generate fmt vet ## Run a controller from your host.
	go run ./cmd/main.go $(KUBECONTEXT_ARG)

# dev-run: build and run the operator locally against the active kubeconfig.
# Stops any existing instance first, waits for ports to free, then starts
# the binary directly (not via go run) so the PID is the binary itself.
# Verifies the health probe responds before returning.
# Usage: make dev-run ARGS="--drift-interval=15s --zap-log-level=1"
DEV_PROBE_PORT ?= 8081
DEV_METRICS_PORT ?= 8080
DEV_OPERATOR_NS ?= cf-edge-operator-system
DEV_LOG ?= /tmp/cf-edge-operator.log

.PHONY: dev-run
dev-run: build ## Build and run locally; stop any existing instance first.
	@echo "Stopping any running instance..."
	@lsof -ti :$(DEV_PROBE_PORT) | xargs kill 2>/dev/null || true
	@for i in $$(seq 1 20); do \
		lsof -ti :$(DEV_PROBE_PORT) >/dev/null 2>&1 || break; \
		echo "  waiting for port $(DEV_PROBE_PORT) to free..."; sleep 0.5; \
	done
	@lsof -ti :$(DEV_PROBE_PORT) >/dev/null 2>&1 \
		&& { echo "ERROR: port $(DEV_PROBE_PORT) still in use"; exit 1; } || true
	@echo "Starting operator (log: $(DEV_LOG))..."
	@bin/cf-edge-operator \
		--operator-namespace=$(DEV_OPERATOR_NS) \
		--metrics-secure=false \
		--metrics-bind-address=:$(DEV_METRICS_PORT) \
		--health-probe-bind-address=:$(DEV_PROBE_PORT) \
		$(KUBECONTEXT_ARG) \
		$(ARGS) \
		> $(DEV_LOG) 2>&1 & echo $$! > /tmp/cf-edge-operator.pid
	@for i in $$(seq 1 20); do \
		curl -sf http://localhost:$(DEV_PROBE_PORT)/healthz >/dev/null 2>&1 && break; \
		sleep 0.5; \
	done
	@curl -sf http://localhost:$(DEV_PROBE_PORT)/healthz >/dev/null 2>&1 \
		|| { echo "ERROR: operator did not become healthy. Check $(DEV_LOG)"; exit 1; }
	@echo "Operator running (PID: $$(cat /tmp/cf-edge-operator.pid))"

.PHONY: dev-stop
dev-stop: ## Stop the locally running operator and verify it is gone.
	@lsof -ti :$(DEV_PROBE_PORT) | xargs kill 2>/dev/null || true
	@for i in $$(seq 1 10); do \
		lsof -ti :$(DEV_PROBE_PORT) >/dev/null 2>&1 || { echo "Stopped"; exit 0; }; \
		sleep 0.5; \
	done
	@lsof -ti :$(DEV_PROBE_PORT) >/dev/null 2>&1 \
		&& { echo "ERROR: port $(DEV_PROBE_PORT) still in use after stop -- something may have restarted it"; exit 1; } \
		|| { echo "Stopped"; exit 0; }

# If you wish to build the manager image targeting other platforms you can use the --platform flag.
# (i.e. docker build --platform linux/arm64). However, you must enable docker buildKit for it.
# More info: https://docs.docker.com/develop/develop-images/build_enhancements/
.PHONY: docker-build
docker-build: ## Build docker image with the manager.
	$(CONTAINER_TOOL) build -t ${IMG} .

.PHONY: docker-push
docker-push: ## Push docker image with the manager.
	$(CONTAINER_TOOL) push ${IMG}

# PLATFORMS defines the target platforms for the manager image be built to provide support to multiple
# architectures. (i.e. make docker-buildx IMG=myregistry/mypoperator:0.0.1). To use this option you need to:
# - be able to use docker buildx. More info: https://docs.docker.com/build/buildx/
# - have enabled BuildKit. More info: https://docs.docker.com/develop/develop-images/build_enhancements/
# - be able to push the image to your registry (i.e. if you do not set a valid value via IMG=<myregistry/image:<tag>> then the export will fail)
# To adequately provide solutions that are compatible with multiple platforms, you should consider using this option.
PLATFORMS ?= linux/arm64,linux/amd64,linux/s390x,linux/ppc64le
.PHONY: docker-buildx
docker-buildx: ## Build and push docker image for the manager for cross-platform support
	# copy existing Dockerfile and insert --platform=${BUILDPLATFORM} into Dockerfile.cross, and preserve the original Dockerfile
	sed -e '1 s/\(^FROM\)/FROM --platform=\$$\{BUILDPLATFORM\}/; t' -e ' 1,// s//FROM --platform=\$$\{BUILDPLATFORM\}/' Dockerfile > Dockerfile.cross
	- $(CONTAINER_TOOL) buildx create --name cf-edge-operator-builder
	$(CONTAINER_TOOL) buildx use cf-edge-operator-builder
	- $(CONTAINER_TOOL) buildx build --push --platform=$(PLATFORMS) --tag ${IMG} -f Dockerfile.cross .
	- $(CONTAINER_TOOL) buildx rm cf-edge-operator-builder
	rm Dockerfile.cross

.PHONY: build-installer
build-installer: manifests generate kustomize ## Generate a consolidated YAML with CRDs and deployment.
	mkdir -p dist
	cd config/manager && "$(KUSTOMIZE)" edit set image controller=${IMG}
	"$(KUSTOMIZE)" build config/default > dist/install.yaml

##@ Deployment

ifndef ignore-not-found
  ignore-not-found = false
endif

.PHONY: install
install: manifests kustomize ## Install CRDs into the K8s cluster specified in ~/.kube/config.
	@out="$$( "$(KUSTOMIZE)" build config/crd 2>/dev/null || true )"; \
	if [ -n "$$out" ]; then echo "$$out" | "$(KUBECTL)" $(KUBECTL_CTX) apply -f -; else echo "No CRDs to install; skipping."; fi

.PHONY: uninstall
uninstall: manifests kustomize ## Uninstall CRDs from the K8s cluster specified in ~/.kube/config. Call with ignore-not-found=true to ignore resource not found errors during deletion.
	@out="$$( "$(KUSTOMIZE)" build config/crd 2>/dev/null || true )"; \
	if [ -n "$$out" ]; then echo "$$out" | "$(KUBECTL)" $(KUBECTL_CTX) delete --ignore-not-found=$(ignore-not-found) -f -; else echo "No CRDs to delete; skipping."; fi

.PHONY: deploy
deploy: manifests kustomize ## Deploy controller to the K8s cluster specified in ~/.kube/config.
	cd config/manager && "$(KUSTOMIZE)" edit set image controller=${IMG}
	"$(KUSTOMIZE)" build config/default | "$(KUBECTL)" $(KUBECTL_CTX) apply -f -

.PHONY: undeploy
undeploy: kustomize ## Undeploy controller from the K8s cluster specified in ~/.kube/config. Call with ignore-not-found=true to ignore resource not found errors during deletion.
	"$(KUSTOMIZE)" build config/default | "$(KUBECTL)" $(KUBECTL_CTX) delete --ignore-not-found=$(ignore-not-found) -f -

##@ Dependencies

## Location to install dependencies to
LOCALBIN ?= $(shell pwd)/bin
$(LOCALBIN):
	mkdir -p "$(LOCALBIN)"

## Tool Binaries
KUBECTL ?= kubectl
# KUBECONTEXT optionally pins the kubeconfig context for kubectl (install/uninstall/
# deploy) and for the out-of-cluster operator (run/dev-run), so these targets never
# depend on the current-context. Empty = use the current-context. Example:
#   make install KUBECONTEXT=kind-cf-lb
KUBECONTEXT ?=
KUBECTL_CTX = $(if $(KUBECONTEXT),--context $(KUBECONTEXT),)
KUBECONTEXT_ARG = $(if $(KUBECONTEXT),--kube-context=$(KUBECONTEXT),)
KIND ?= kind
KUSTOMIZE ?= $(LOCALBIN)/kustomize
CONTROLLER_GEN ?= $(LOCALBIN)/controller-gen
ENVTEST ?= $(LOCALBIN)/setup-envtest
GOLANGCI_LINT = $(LOCALBIN)/golangci-lint
ACTIONLINT ?= $(LOCALBIN)/actionlint
SHELLCHECK ?= $(LOCALBIN)/shellcheck

## Tool Versions
# Pinned tool versions (hardcoded).
KUSTOMIZE_VERSION ?= v5.8.1
CONTROLLER_TOOLS_VERSION ?= v0.20.1
GOLANGCI_LINT_VERSION ?= v2.8.0
ACTIONLINT_VERSION ?= v1.7.7
SHELLCHECK_VERSION ?= v0.10.0

# Versions inferred from go.mod so envtest stays in lockstep with the module deps.
#ENVTEST_VERSION is the version of controller-runtime release branch to fetch the envtest setup script (i.e. release-0.20)
ENVTEST_VERSION ?= $(shell v='$(call gomodver,sigs.k8s.io/controller-runtime)'; \
  [ -n "$$v" ] || { echo "Set ENVTEST_VERSION manually (controller-runtime replace has no tag)" >&2; exit 1; }; \
  printf '%s\n' "$$v" | sed -E 's/^v?([0-9]+)\.([0-9]+).*/release-\1.\2/')

#ENVTEST_K8S_VERSION is the version of Kubernetes to use for setting up ENVTEST binaries (i.e. 1.31)
ENVTEST_K8S_VERSION ?= $(shell v='$(call gomodver,k8s.io/api)'; \
  [ -n "$$v" ] || { echo "Set ENVTEST_K8S_VERSION manually (k8s.io/api replace has no tag)" >&2; exit 1; }; \
  printf '%s\n' "$$v" | sed -E 's/^v?[0-9]+\.([0-9]+).*/1.\1/')
.PHONY: kustomize
kustomize: $(KUSTOMIZE) ## Download kustomize locally if necessary.
$(KUSTOMIZE): $(LOCALBIN)
	$(call go-install-tool,$(KUSTOMIZE),sigs.k8s.io/kustomize/kustomize/v5,$(KUSTOMIZE_VERSION))

.PHONY: controller-gen
controller-gen: $(CONTROLLER_GEN) ## Download controller-gen locally if necessary.
$(CONTROLLER_GEN): $(LOCALBIN)
	$(call go-install-tool,$(CONTROLLER_GEN),sigs.k8s.io/controller-tools/cmd/controller-gen,$(CONTROLLER_TOOLS_VERSION))

.PHONY: setup-envtest
setup-envtest: envtest ## Download the binaries required for ENVTEST in the local bin directory.
	@echo "Setting up envtest binaries for Kubernetes version $(ENVTEST_K8S_VERSION)..."
	@"$(ENVTEST)" use $(ENVTEST_K8S_VERSION) --bin-dir "$(LOCALBIN)" -p path || { \
		echo "Error: Failed to set up envtest binaries for version $(ENVTEST_K8S_VERSION)."; \
		exit 1; \
	}

.PHONY: envtest
envtest: $(ENVTEST) ## Download setup-envtest locally if necessary.
$(ENVTEST): $(LOCALBIN)
	$(call go-install-tool,$(ENVTEST),sigs.k8s.io/controller-runtime/tools/setup-envtest,$(ENVTEST_VERSION))

.PHONY: golangci-lint
golangci-lint: $(GOLANGCI_LINT) ## Download golangci-lint locally if necessary.
$(GOLANGCI_LINT): $(LOCALBIN)
	$(call go-install-tool,$(GOLANGCI_LINT),github.com/golangci/golangci-lint/v2/cmd/golangci-lint,$(GOLANGCI_LINT_VERSION))
	@test -f .custom-gcl.yml && { \
		echo "Building custom golangci-lint with plugins..." && \
		$(GOLANGCI_LINT) custom --destination $(LOCALBIN) --name golangci-lint-custom && \
		mv -f $(LOCALBIN)/golangci-lint-custom "$(GOLANGCI_LINT)-$(GOLANGCI_LINT_VERSION)" && \
		ln -sf "$$(realpath "$(GOLANGCI_LINT)-$(GOLANGCI_LINT_VERSION)")" "$(GOLANGCI_LINT)"; \
	} || true

.PHONY: actionlint
actionlint: $(ACTIONLINT) ## Download actionlint locally if necessary.
$(ACTIONLINT): $(LOCALBIN)
	$(call go-install-tool,$(ACTIONLINT),github.com/rhysd/actionlint/cmd/actionlint,$(ACTIONLINT_VERSION))

.PHONY: shellcheck
shellcheck: $(SHELLCHECK) ## Download a pinned shellcheck static binary locally if necessary.
$(SHELLCHECK): $(LOCALBIN)
	$(call download-shellcheck,$(SHELLCHECK),$(SHELLCHECK_VERSION))

# download-shellcheck fetches a pinned shellcheck static binary (shellcheck is not
# go-installable). $1 - target path with name; $2 - version (vX.Y.Z). Darwin uses the
# x86_64 build (runs on arm64 via Rosetta) to avoid depending on a darwin.aarch64 asset.
define download-shellcheck
@[ -f "$(1)-$(2)" ] && [ "$$(readlink -- "$(1)" 2>/dev/null)" = "$(1)-$(2)" ] || { \
set -e; \
os=$$(uname -s | tr '[:upper:]' '[:lower:]'); \
if [ "$$os" = "darwin" ]; then arch=x86_64; \
else case "$$(uname -m)" in aarch64|arm64) arch=aarch64 ;; *) arch=x86_64 ;; esac; fi; \
url="https://github.com/koalaman/shellcheck/releases/download/$(2)/shellcheck-$(2).$${os}.$${arch}.tar.xz"; \
echo "Downloading $${url}"; \
tmp=$$(mktemp -d); \
curl -sSfL "$${url}" | tar -xJ -C "$${tmp}"; \
mv "$${tmp}/shellcheck-$(2)/shellcheck" "$(1)-$(2)"; \
chmod +x "$(1)-$(2)"; \
rm -rf "$${tmp}"; \
} ;\
ln -sf "$$(realpath "$(1)-$(2)")" "$(1)"
endef

# go-install-tool will 'go install' any package with custom target and name of binary, if it doesn't exist
# $1 - target path with name of binary
# $2 - package url which can be installed
# $3 - specific version of package
define go-install-tool
@[ -f "$(1)-$(3)" ] && [ "$$(readlink -- "$(1)" 2>/dev/null)" = "$(1)-$(3)" ] || { \
set -e; \
package=$(2)@$(3) ;\
echo "Downloading $${package}" ;\
rm -f "$(1)" ;\
GOBIN="$(LOCALBIN)" go install $${package} ;\
mv "$(LOCALBIN)/$$(basename "$(1)")" "$(1)-$(3)" ;\
} ;\
ln -sf "$$(realpath "$(1)-$(3)")" "$(1)"
endef

define gomodver
$(shell go list -m -f '{{if .Replace}}{{.Replace.Version}}{{else}}{{.Version}}{{end}}' $(1) 2>/dev/null)
endef
