#!/usr/bin/env bash
# Does removing the checkpoint eliminate the malformed snapshots?
#
# Two test binaries, one per tree, so there is no env gating to get wrong -- an argument
# ordering bug in a previous harness (GNU env treats the first VAR=value as the end of
# options) meant the non-default arm never ran the test and its failures were counted as
# corruption. Nothing here passes variables to select behaviour.
#
# Every failure is classified by which of the test's four assertions fired, because
# counting any non-pass as "corrupt" is what produced the wrong conclusion before.
set -u
FIX=$1
BASE=$2
GATED=$3
TEST=commits_under_a_concurrent_writer_capture_only_whole_transactions

# Guard: the binaries must differ, or this compares one thing twice.
cmp -s "$FIX" "$BASE" && { echo "::error::fix and base are identical"; exit 1; }
cmp -s "$GATED" "$BASE" && { echo "::error::gated and base are identical"; exit 1; }
echo "  three binaries: fix $(md5sum "$FIX" | cut -c1-10), base $(md5sum "$BASE" | cut -c1-10), gated $(md5sum "$GATED" | cut -c1-10)"

declare -A INTEG ROWS TORN GAP OTHER PASS
for a in fix base gated; do INTEG[$a]=0; ROWS[$a]=0; TORN[$a]=0; GAP[$a]=0; OTHER[$a]=0; PASS[$a]=0; done

run() { # $1 = binary, $2 = optional env
  local out
  if [ "${2:-}" = gate ]; then
    out=$(env ARM_C=1 "$1" "$TEST" --exact --nocapture 2>&1)
  else
    out=$("$1" "$TEST" --exact --nocapture 2>&1)
  fi
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
  for a in base gated fix; do
    case $a in
      base)  line=$(run "$BASE") ;;
      gated) line=$(run "$GATED" gate) ;;
      fix)   line=$(run "$FIX") ;;
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
for a in base gated fix; do
  case $a in
    base)  l="main, checkpoint present" ;;
    gated) l="main, ARM_C=1 skips ckpt" ;;
    fix)   l="source removal" ;;
  esac
  printf "  %-28s %6s %10s %6s %5s %6s %6s\n" "$l" "${PASS[$a]}" "${INTEG[$a]}" "${TORN[$a]}" "${GAP[$a]}" "${ROWS[$a]}" "${OTHER[$a]}"
done
# Guard: if main did not reproduce, there is nothing to show improvement against.
[ "${INTEG[base]}" -gt 0 ] || echo "::warning::main produced no malformed snapshot in $N attempts; the comparison is empty"
