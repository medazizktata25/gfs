#!/usr/bin/env bash
# Does removing the WAL checkpoint eliminate the malformed snapshots?
#
# THE BEHAVIOUR LIVES IN THE gfs BINARY, NOT THE TEST BINARY. The test invokes
# env!("CARGO_BIN_EXE_gfs"), a COMPILE-TIME path, so every test binary runs the same
# target/debug/gfs on disk -- whichever was built last. Two earlier runs of this script
# therefore compared test binaries while all of them shared one gfs, and scored main's
# behaviour under the fix's name.
#
# So this swaps the gfs binary per arm and keeps ONE test binary. Each swap is verified
# by md5 before the arm runs.
set -u
TESTBIN=$1
GFS_FIX=$2
GFS_BASE=$3
GFS_PATH=$4   # the compile-time path the test binary will invoke
TEST=commits_under_a_concurrent_writer_capture_only_whole_transactions

cmp -s "$GFS_FIX" "$GFS_BASE" && { echo "::error::the two gfs binaries are identical"; exit 1; }
echo "  gfs fix   $(md5sum "$GFS_FIX" | cut -c1-12)"
echo "  gfs base  $(md5sum "$GFS_BASE" | cut -c1-12)"
echo "  test bin  $(md5sum "$TESTBIN" | cut -c1-12)  invokes $GFS_PATH"

use() { # $1 = which gfs to install; verified, because an unswapped binary is the bug above
  cp "$1" "$GFS_PATH"
  local want have
  want=$(md5sum "$1" | cut -d' ' -f1)
  have=$(md5sum "$GFS_PATH" | cut -d' ' -f1)
  [ "$want" = "$have" ] || { echo "::error::gfs swap did not take effect"; exit 1; }
}

declare -A INTEG ROWS TORN GAP OTHER PASS
for a in fix base; do INTEG[$a]=0; ROWS[$a]=0; TORN[$a]=0; GAP[$a]=0; OTHER[$a]=0; PASS[$a]=0; done

run() { # $1 = which gfs binary to run against
  use "$1"
  local out
  out=$("$TESTBIN" "$TEST" --exact --nocapture 2>&1)
  if printf '%s' "$out" | grep -q '^test result: ok'; then echo PASS; return; fi
  # An exec or harness failure is not a test failure.
  if printf '%s' "$out" | grep -qE "No such file|Permission denied|error: test failed, to rerun"; then
    if ! printf '%s' "$out" | grep -q 'panicked at'; then
      echo "::error::the binary did not run the test: $(printf '%s' "$out" | head -1)" >&2
      exit 1
    fi
  fi
  # The message, not the panic LOCATION. 'panicked at ...' appears first in the output,
  # so including it as an alternative shadowed the message and put every classified
  # failure into 'other'.
  local msg
  msg=$(printf '%s' "$out" | grep -oE 'the snapshot guard did not hold.*' | head -1)
  [ -n "$msg" ] || msg=$(printf '%s' "$out" | grep -A2 'panicked at' | tail -1 | cut -c1-160)
  echo "$msg"
}

N=40
echo "=== interleaving $N attempts per binary ==="
for i in $(seq 1 $N); do
  for a in base fix; do
    case $a in
      base) line=$(run "$GFS_BASE") ;;
      fix)  line=$(run "$GFS_FIX") ;;
    esac
    if [ "$line" = PASS ]; then PASS[$a]=$(( ${PASS[$a]} + 1 )); printf '.'; continue; fi
    printf 'F'
    case "$line" in
      *"integrity_check said"*) INTEG[$a]=$(( ${INTEG[$a]} + 1 )) ;;
      *"torn transaction"*)     TORN[$a]=$((  ${TORN[$a]} + 1 ))  ;;
      *"batch gap"*)            GAP[$a]=$((   ${GAP[$a]} + 1 ))   ;;
      *"rows, outside"*)        ROWS[$a]=$((  ${ROWS[$a]} + 1 ))  ;;
      *)                        OTHER[$a]=$(( ${OTHER[$a]} + 1 )); [ "$i" -le 3 ] && { echo; echo "    other on $a: $(printf '%s' "$line" | cut -c1-140)"; } ;;
    esac
  done
done
echo
echo "################ RESULT (of $N each) ################"
printf "  %-28s %6s %10s %6s %5s %6s %6s\n" binary pass integrity torn gap rows other
for a in base fix; do
  case $a in
    base) l="gfs with the checkpoint" ;;
    fix)  l="gfs without it" ;;
  esac
  printf "  %-28s %6s %10s %6s %5s %6s %6s\n" "$l" "${PASS[$a]}" "${INTEG[$a]}" "${TORN[$a]}" "${GAP[$a]}" "${ROWS[$a]}" "${OTHER[$a]}"
done
# Guard: if main did not reproduce, there is nothing to show improvement against.
[ "${INTEG[base]}" -gt 0 ] || echo "::warning::main produced no malformed snapshot in $N attempts; the comparison is empty"
