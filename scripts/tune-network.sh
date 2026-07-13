#!/usr/bin/env bash
#
# EC2 network tuning for the S3 throughput benchmark (faster TCP ramp-up + higher
# per-connection ceiling). Run ON the EC2 instance, as root:
#
#     sudo ./scripts/tune-network.sh            # apply
#     sudo ./scripts/tune-network.sh --revert   # restore the exact prior values
#
# Applies settings at runtime AND persists them to /etc/sysctl.d so they survive
# reboot. Before changing anything it SNAPSHOTS the current values, so --revert
# restores precisely what was there (not guessed defaults). Idempotent: re-running
# apply won't overwrite the original snapshot.
#
# Tuned for a c7gn.16xlarge (200 Gbps ENA) in us-west-2 pulling from S3. These are
# host-wide kernel settings; this box is assumed to be a dedicated benchmark host.
#
# What it changes and why:
#   tcp_slow_start_after_idle=0  Don't reset the congestion window after a brief
#                                idle. Part fetching is bursty (gaps between parts,
#                                esp. ordered-stream), and the default re-ramps
#                                every connection back to slow-start. Biggest
#                                ramp-up win for this workload.
#   tcp_congestion_control=bbr   Ramp to available bandwidth faster than CUBIC and
#                                tolerate loss better on high-bandwidth paths.
#   default_qdisc=fq             BBR wants fair-queuing pacing to behave well.
#   rmem/wmem + tcp_rmem/tcp_wmem  Large socket buffers so the TCP window can open
#                                enough to fill the bandwidth-delay product.
#   netdev_max_backlog, tcp_mtu_probing  Headroom for high packet rates / PMTU.
#   initcwnd/initrwnd=30 (route) Send ~30 segments in the first RTT instead of 10,
#                                shortening the cold-start ramp on new connections.
#
set -euo pipefail

SYSCTL_FILE=/etc/sysctl.d/99-s3bench.conf
BACKUP_FILE=/etc/sysctl.d/.s3bench-backup.env   # original values, saved on apply

# sysctl keys this script modifies (order doesn't matter).
KEYS=(
  net.ipv4.tcp_slow_start_after_idle
  net.core.default_qdisc
  net.ipv4.tcp_congestion_control
  net.core.rmem_max
  net.core.wmem_max
  net.ipv4.tcp_rmem
  net.ipv4.tcp_wmem
  net.core.netdev_max_backlog
  net.ipv4.tcp_mtu_probing
)

require_root() {
  if [[ $EUID -ne 0 ]]; then
    echo "Please run as root: sudo $0 ${*:-}" >&2
    exit 1
  fi
}

# ---------------------------------------------------------------------------
# Revert: restore the exact snapshot taken at apply time, then clean up.
# ---------------------------------------------------------------------------
if [[ "${1:-}" == "--revert" ]]; then
  require_root --revert
  echo "Reverting network tuning ..."
  rm -f "${SYSCTL_FILE}"

  if [[ -f "${BACKUP_FILE}" ]]; then
    # Each line is "key<TAB>value..."; restore verbatim.
    while IFS=$'\t' read -r key value; do
      [[ -z "${key}" ]] && continue
      if sysctl -w "${key}=${value}" >/dev/null 2>&1; then
        echo "  restored ${key} = ${value}"
      else
        echo "  WARN: could not restore ${key}" >&2
      fi
    done < "${BACKUP_FILE}"
    rm -f "${BACKUP_FILE}"
  else
    echo "  No snapshot found (${BACKUP_FILE}); restoring conservative defaults."
    sysctl -w net.ipv4.tcp_slow_start_after_idle=1 >/dev/null || true
    sysctl -w net.ipv4.tcp_congestion_control=cubic >/dev/null || true
  fi

  # Drop initcwnd/initrwnd back to kernel defaults by re-issuing the route
  # without them (best-effort; a reboot also clears it).
  DEV=$(ip route show default 2>/dev/null | awk '{for(i=1;i<=NF;i++) if($i=="dev"){print $(i+1); exit}}')
  GW=$(ip route show default 2>/dev/null | awk '{for(i=1;i<=NF;i++) if($i=="via"){print $(i+1); exit}}')
  if [[ -n "${DEV:-}" && -n "${GW:-}" ]]; then
    ip route change default via "${GW}" dev "${DEV}" 2>/dev/null \
      && echo "  reset initcwnd/initrwnd on default route" \
      || echo "  note: could not reset route initcwnd (reboot clears it)"
  fi

  echo "Revert complete."
  exit 0
fi

# ---------------------------------------------------------------------------
# Apply.
# ---------------------------------------------------------------------------
require_root

# Snapshot originals ONCE (first apply), so --revert can restore them exactly.
if [[ ! -f "${BACKUP_FILE}" ]]; then
  echo "Snapshotting current values to ${BACKUP_FILE} ..."
  : > "${BACKUP_FILE}"
  chmod 600 "${BACKUP_FILE}"
  for key in "${KEYS[@]}"; do
    if val=$(sysctl -n "${key}" 2>/dev/null); then
      printf '%s\t%s\n' "${key}" "${val}" >> "${BACKUP_FILE}"
    fi
  done
else
  echo "Snapshot already exists (${BACKUP_FILE}); keeping original values."
fi

echo
echo "== Before =="
sysctl net.ipv4.tcp_slow_start_after_idle net.ipv4.tcp_congestion_control net.core.default_qdisc 2>/dev/null || true
echo

# BBR needs its module; modern kernels (AL2023, recent Ubuntu) ship it built-in.
modprobe tcp_bbr 2>/dev/null || true

# Persist for reboot.
cat > "${SYSCTL_FILE}" <<'EOF'
# S3 throughput benchmark network tuning (see scripts/tune-network.sh)
net.ipv4.tcp_slow_start_after_idle = 0
net.core.default_qdisc = fq
net.ipv4.tcp_congestion_control = bbr

# Large socket buffers for high bandwidth-delay product.
net.core.rmem_max = 134217728
net.core.wmem_max = 134217728
net.ipv4.tcp_rmem = 4096 87380 134217728
net.ipv4.tcp_wmem = 4096 65536 134217728

# Headroom for high packet rates and PMTU discovery.
net.core.netdev_max_backlog = 250000
net.ipv4.tcp_mtu_probing = 1
EOF

# Apply now.
sysctl -p "${SYSCTL_FILE}"

# Larger initial congestion/receive window on the default route. Not persisted by
# sysctl (it's a route attribute); re-run after reboot if you need it. Best-effort.
DEV=$(ip route show default 2>/dev/null | awk '{for(i=1;i<=NF;i++) if($i=="dev"){print $(i+1); exit}}')
GW=$(ip route show default 2>/dev/null | awk '{for(i=1;i<=NF;i++) if($i=="via"){print $(i+1); exit}}')
if [[ -n "${DEV:-}" && -n "${GW:-}" ]]; then
  if ip route change default via "${GW}" dev "${DEV}" initcwnd 30 initrwnd 30 2>/dev/null; then
    echo "Set initcwnd/initrwnd=30 on default route (dev ${DEV}, via ${GW})."
  else
    echo "Note: could not set initcwnd on the default route (non-fatal)."
  fi
else
  echo "Note: default route not in expected form; skipped initcwnd (non-fatal)."
fi

echo
echo "== After =="
sysctl net.ipv4.tcp_slow_start_after_idle net.ipv4.tcp_congestion_control net.core.default_qdisc
echo
echo "Done. Verify BBR is active with: sysctl net.ipv4.tcp_congestion_control"
echo "(If it still says cubic, the kernel lacks the tcp_bbr module.)"
echo "Undo any time with: sudo $0 --revert"
