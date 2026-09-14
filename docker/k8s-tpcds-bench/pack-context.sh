#!/usr/bin/env bash
#
# Packs everything the isolated segment needs into one archive: the wheels and
# jar that fetch-deps.sh downloaded, plus the Dockerfile and the scripts that
# use them. Run it on the machine that has internet, after fetch-deps.sh.
#
#   ./pack-context.sh
#
# gzip, not xz or zstd: the target CentOS 8 host has neither, and the archive is
# only tens of megabytes, so the weaker compression costs nothing that matters.
#
# A SHA256SUMS file travels inside the archive and a .sha256 beside it, because
# the transfer is the step most likely to corrupt this silently.

set -euo pipefail
cd "$(dirname "$0")"

NAME=tpcds-bench-context
: "${OUT:=${NAME}-$(date +%Y%m%d).tar.gz}"

for d in wheels jars; do
  [ -d "$d" ] || { echo "missing $d/ -- run fetch-deps.sh first" >&2; exit 1; }
  [ -n "$(ls -A "$d")" ] || { echo "$d/ is empty -- run fetch-deps.sh first" >&2; exit 1; }
done

# The binary wheels are built for one CPython ABI, and installing them into an
# image with a different Python fails at import time rather than at build time.
# Recording the ABI here lets build-image.sh check it against the base image.
ABIS=$(ls wheels/*.whl | grep -oE 'cp3[0-9]+' | sort -u || true)
case $(printf '%s\n' "$ABIS" | grep -c .) in
  0) echo "note: no binary wheels found; nothing to pin to a Python version" >&2
     PYVER="" ;;
  1) PYVER="3.${ABIS#cp3}" ;;
  *) echo "wheels mix several Python ABIs: $(echo "$ABIS" | tr '\n' ' ')" >&2
     echo "re-run fetch-deps.sh with a single PYVER" >&2; exit 1 ;;
esac

STAGE=$(mktemp -d)
trap 'rm -rf "$STAGE"' EXIT
mkdir -p "$STAGE/$NAME"

cp -r wheels jars "$STAGE/$NAME/"
cp Dockerfile build-image.sh run-tpcds-bench.sh smoke-test.sh smoke.env.example \
   tuning-sf1000.conf run-streams.sh \
   README.md "$STAGE/$NAME/"

{
  echo "TPC-DS benchmark image build context"
  echo "packed:        $(date -u +%Y-%m-%dT%H:%M:%SZ) on $(hostname)"
  echo "python ABI:    ${PYVER:-none (pure-python wheels only)}"
  echo
  echo "The base image must run Python ${PYVER:-any}; build-image.sh checks this."
  echo
  echo "Contents:"
  ( cd "$STAGE/$NAME" && find wheels jars -type f | sort | sed 's/^/  /' )
} > "$STAGE/$NAME/MANIFEST.txt"

( cd "$STAGE/$NAME" && find wheels jars -type f | sort | xargs sha256sum > SHA256SUMS )

tar -czf "$OUT" --owner=0 --group=0 -C "$STAGE" "$NAME"
sha256sum "$OUT" > "$OUT.sha256"

echo
echo "Wrote $OUT ($(du -h "$OUT" | cut -f1)) and $OUT.sha256"
echo "Python ABI pinned by the wheels: ${PYVER:-none}"
echo
echo "Transfer both files, then inside the isolated segment:"
echo "  sha256sum -c $OUT.sha256"
echo "  tar -xzf $OUT && cd $NAME"
echo "  BASE_IMAGE=... TARGET_IMAGE=... ./build-image.sh"
