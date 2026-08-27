#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 <flink-home>" >&2
  exit 2
fi

flink_home=$1
project_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
mapfile -t streamfusion_jars < <(find "$project_root/streamfusion-flink/target" -maxdepth 1 -type f -name 'streamfusion-flink-*.jar')
mapfile -t job_jars < <(find "$project_root/streamfusion-flink-runner-tests/target" -maxdepth 1 -type f -name 'streamfusion-flink-runner-tests-*.jar')
mapfile -t installed_api_jars < <(find "$flink_home/lib" -maxdepth 1 -type f -name 'flink-table-api-java-*.jar')
if [[ ${#installed_api_jars[@]} -ne 1 ]]; then
  echo "Expected exactly one Flink table API JAR, found ${#installed_api_jars[@]}" >&2
  exit 1
fi
patched_api_jar="$project_root/flink/flink-table/flink-table-api-java-uber/target/$(basename "${installed_api_jars[0]}")"

if [[ ${#streamfusion_jars[@]} -ne 1 ]]; then
  echo "Expected exactly one StreamFusion JAR, found ${#streamfusion_jars[@]}" >&2
  exit 1
fi
if [[ ${#job_jars[@]} -ne 1 ]]; then
  echo "Expected exactly one runner job JAR, found ${#job_jars[@]}" >&2
  exit 1
fi
if [[ ! -f "$patched_api_jar" ]]; then
  echo "Patched Flink table API JAR does not exist: $patched_api_jar" >&2
  exit 1
fi

cp "$patched_api_jar" "${installed_api_jars[0]}"
cp "${streamfusion_jars[0]}" "$flink_home/lib/"

runner_output=$(mktemp)
trap 'rm -f "$runner_output"' EXIT
"$flink_home/bin/flink" run -t local \
  -c tech.streamfusion.flink.runner.PlannerHookIntegrationJob \
  "${job_jars[0]}" | tee "$runner_output"

grep -q 'STREAMFUSION_RUNNER_INTEGRATION_OK planners=1 translations=' "$runner_output"
