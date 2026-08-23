# Contributing

Thanks for your interest in cf-edge-operator. This is a Kubernetes operator that
manages Cloudflare for SaaS custom hostnames (and their zones) declaratively.
Bug reports, feature requests, and pull requests are all welcome.

## Getting started

Prerequisites:

- Go (see [.go-version](.go-version))
- Docker (for building images and running the e2e suite)
- A [Kind](https://kind.sigs.k8s.io/) cluster for end-to-end tests

Build tooling (controller-gen, kustomize, setup-envtest, golangci-lint) is
downloaded into `bin/` automatically by the relevant `make` targets, so you do
not need to install it by hand. Run `make help` to see every target.

## Development loop

```sh
make build        # compile the manager
make test         # unit + integration tests (envtest: real API server + etcd)
make lint         # golangci-lint
make lint-fix     # auto-fix what it can
make run          # run against your current kubeconfig context
make test-e2e     # full e2e against a dedicated Kind cluster
```

When running out-of-cluster (`make run` / `make dev-run`), pass
`KUBECONTEXT=<context>` to pin the target cluster instead of relying on the
current-context (it maps to the operator's `--kube-context` flag); kubectl-based
targets like `make install` honor the same variable. In-cluster the operator
always uses its pod ServiceAccount, so `--kube-context` is ignored there.

## Code generation

CRDs, RBAC, and DeepCopy methods are generated from `+kubebuilder` markers.
After editing any `api/**/*_types.go` file or its markers, regenerate and sync the
generated CRDs into the chart:

```sh
make manifests generate
make helm-crd-sync
```

Do not hand-edit generated files; your changes will be overwritten. These are
generated:

- `api/**/zz_generated.deepcopy.go`
- `config/crd/bases/*.yaml` (source of truth), copied into
  `charts/cf-edge-operator/crds-render/*.yaml` by `make helm-crd-sync`
- `config/rbac/role.yaml`
- `PROJECT`

`make helm-crd-diff` verifies the chart CRDs match the generated ones.

## Conventions

- Tests are Ginkgo + Gomega; controller tests use envtest. New behavior should
  come with tests.
- `make test` and `make lint` must pass before a PR is merged.
- Keep source, docs, and commit messages ASCII-only (plain `-`, `->`, `"`), not
  typographic punctuation.
- Commit messages follow [Conventional Commits](https://www.conventionalcommits.org/)
  (`feat:`, `fix:`, `chore:`, `docs:`, `refactor:`), imperative mood.

## Pull requests

1. Branch from `main`.
2. Make the change; run `make test` and `make lint`.
3. Open a PR describing what changed and why. Keep it focused; a clean,
   reviewable diff merges faster.

By contributing, you agree that your contributions are licensed under the
project's [Apache License 2.0](LICENSE).
