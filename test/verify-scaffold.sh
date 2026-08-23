#!/usr/bin/env bash
# Verify every CRD is wired consistently across the hand-maintained "list of all
# kinds" files -- the ones controller-gen does NOT regenerate (only `kubebuilder
# create api` appends them), so an API added by hand drifts out of them silently.
# None of these are exercised by the rest of the gate: envtest loads
# config/crd/bases/ directly, and the Helm chart installs from crds-render/.
#
# For each config/crd/bases/*.yaml this asserts:
#   1. it is listed in config/crd/kustomization.yaml   (else make install/deploy skip the CRD)
#   2. its kind has admin/editor/viewer helper roles in config/rbac/ that are listed
#      in config/rbac/kustomization.yaml
#   3. its kind has a PROJECT entry under the correct group
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.." || exit 1

crd_kust="config/crd/kustomization.yaml"
rbac_kust="config/rbac/kustomization.yaml"
fail=0
count=0

for base in config/crd/bases/*.yaml; do
  count=$((count + 1))
  fname="$(basename "${base}")"
  group="${fname%%.cf-edge.io_*}"
  kind="$(awk '/^    kind:/{print $2; exit}' "${base}")"
  singular="$(awk '/^    singular:/{print $2; exit}' "${base}")"

  # 1. CRD listed in the crd kustomization resources list
  if ! grep -qF "bases/${fname}" "${crd_kust}"; then
    echo "FAIL: ${fname} is in config/crd/bases/ but not listed in ${crd_kust}" >&2
    fail=1
  fi

  # 2. admin/editor/viewer helper roles exist and are listed in the rbac kustomization
  if [[ -z "${singular}" ]]; then
    echo "FAIL: could not read spec.names.singular from ${base}" >&2
    fail=1
  else
    for role in admin editor viewer; do
      rf="${singular}_${role}_role.yaml"
      if [[ ! -f "config/rbac/${rf}" ]]; then
        echo "FAIL: missing helper role config/rbac/${rf} for kind '${singular}'" >&2
        fail=1
      elif ! grep -qF "${rf}" "${rbac_kust}"; then
        echo "FAIL: config/rbac/${rf} exists but is not listed in ${rbac_kust}" >&2
        fail=1
      fi
    done
  fi

  # 3. PROJECT entry present under the correct group
  if [[ -z "${kind}" ]]; then
    echo "FAIL: could not read spec.names.kind from ${base}" >&2
    fail=1
  else
    proj_group="$(awk -v k="${kind}" '/^  group:/{g=$2} $0=="  kind: " k {print g; exit}' PROJECT)"
    if [[ -z "${proj_group}" ]]; then
      echo "FAIL: kind ${kind} has no entry in PROJECT" >&2
      fail=1
    elif [[ "${proj_group}" != "${group}" ]]; then
      echo "FAIL: PROJECT lists ${kind} under group '${proj_group}', expected '${group}'" >&2
      fail=1
    fi
  fi
done

if [[ "${fail}" -ne 0 ]]; then
  {
    echo ""
    echo "scaffold verification FAILED -- a CRD is not wired into every hand-maintained"
    echo "list. After adding an API by hand (without 'kubebuilder create api'), update:"
    echo "  - ${crd_kust}   (add '- bases/<group>_<plural>.yaml')"
    echo "  - config/rbac/  (<singular>_{admin,editor,viewer}_role.yaml) + ${rbac_kust}"
    echo "  - PROJECT       (resources entry with the correct group/kind/path)"
  } >&2
  exit 1
fi

echo "scaffold OK: ${count} CRDs wired into config/crd + config/rbac helper roles + PROJECT"
