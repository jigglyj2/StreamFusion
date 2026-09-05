#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 <flink-source-home>" >&2
  exit 2
fi

flink_source_home=$1
project_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
streamfusion_version=$(
  cd "$project_root"
  mvn help:evaluate -Dexpression=revision -q -DforceStdout
)

if [[ ! -x "$flink_source_home/mvnw" ]]; then
  echo "Flink Maven wrapper does not exist: $flink_source_home/mvnw" >&2
  exit 1
fi

audit_dir="$flink_source_home/target/streamfusion-sql-suite"
mkdir -p "$audit_dir"

run_suite() {
  local suite=$1
  local audit_file="$audit_dir/${suite}-acceleration-audit.log"
  rm -f "$audit_file"
  (
    cd "$flink_source_home"
    JAVA_TOOL_OPTIONS="${JAVA_TOOL_OPTIONS:-} -Dtech.streamfusion.flink.planner.factory=tech.streamfusion.flink.StreamFusionPlannerFactory -Dtech.streamfusion.flink.acceleration-audit-file=$audit_file" \
      ./mvnw --batch-mode \
        -pl flink-table/flink-table-planner \
        -Pstreamfusion-sql-suite \
        -Dstreamfusion.version="$streamfusion_version" \
        -Dstreamfusion.sql.includes="**/runtime/${suite}/**/*ITCase.*" \
        -DfailIfNoTests=true \
        test-compile surefire:test@integration-tests
  )
  if [[ ! -s "$audit_file" ]] || ! grep -q '^accelerated$' "$audit_file"; then
    echo "Flink's ${suite} SQL suite did not exercise an accelerated StreamFusion plan" >&2
    exit 1
  fi
}

run_suite stream
run_suite batch
