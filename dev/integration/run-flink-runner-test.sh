#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 <flink-home>" >&2
  exit 2
fi

flink_home=$1
project_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
flink_source_home=${FLINK_SOURCE_HOME:-"$project_root/flink"}
mapfile -t streamfusion_jars < <(find "$project_root/streamfusion-flink/target" -maxdepth 1 -type f -name 'streamfusion-flink-*.jar')
mapfile -t planner_extension_jars < <(find "$project_root/streamfusion-flink-planner/target" -maxdepth 1 -type f -name 'streamfusion-flink-planner-*.jar')
mapfile -t job_jars < <(find "$project_root/streamfusion-flink-runner-tests/target" -maxdepth 1 -type f -name 'streamfusion-flink-runner-tests-*.jar')
mapfile -t installed_api_jars < <(find "$flink_home/lib" -maxdepth 1 -type f -name 'flink-table-api-java-*.jar')
mapfile -t planner_loader_jars < <(find "$flink_home/lib" -maxdepth 1 -type f -name 'flink-table-planner-loader-*.jar')
if [[ ${#installed_api_jars[@]} -ne 1 ]]; then
  echo "Expected exactly one Flink table API JAR, found ${#installed_api_jars[@]}" >&2
  exit 1
fi
patched_api_jar="$flink_source_home/flink-table/flink-table-api-java-uber/target/$(basename "${installed_api_jars[0]}")"

if [[ ${#streamfusion_jars[@]} -ne 1 ]]; then
  echo "Expected exactly one StreamFusion JAR, found ${#streamfusion_jars[@]}" >&2
  exit 1
fi
if [[ ${#planner_extension_jars[@]} -ne 1 ]]; then
  echo "Expected exactly one StreamFusion planner extension JAR, found ${#planner_extension_jars[@]}" >&2
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
if [[ ${#planner_loader_jars[@]} -ne 1 ]]; then
  echo "Expected exactly one distribution planner loader JAR" >&2
  exit 1
fi

cp "$patched_api_jar" "${installed_api_jars[0]}"
planner_staging=$(mktemp -d)
mkdir -p "$planner_staging/org/apache/flink/table/factories"
cp "$flink_source_home/flink-table/flink-table-api-java/target/classes/org/apache/flink/table/factories/PlannerFactoryUtil.class" \
  "$planner_staging/org/apache/flink/table/factories/"
jar --update --file "${installed_api_jars[0]}" \
  -C "$planner_staging" org/apache/flink/table/factories/PlannerFactoryUtil.class
(
  cd "$planner_staging"
  jar --extract --file "${planner_loader_jars[0]}" flink-table-planner.jar
  mkdir -p org/apache/flink/table/planner/delegation
  cp "$flink_source_home/flink-table/flink-table-planner/target/classes/org/apache/flink/table/planner/delegation/PlannerBase.class" \
    org/apache/flink/table/planner/delegation/
  # The runner directory may already have been prepared by an earlier local invocation.
  zip -dq flink-table-planner.jar META-INF/versions/9/module-info.class || true
  jar --update --file flink-table-planner.jar \
    org/apache/flink/table/planner/delegation/PlannerBase.class
  jar --update --file flink-table-planner.jar \
    -C "$project_root/streamfusion-flink-planner/target/classes" tech/streamfusion/flink/planner
)
jar --update --file "${planner_loader_jars[0]}" -C "$planner_staging" flink-table-planner.jar
cp "${streamfusion_jars[0]}" "$flink_home/lib/"

runner_output=$(mktemp)
trap 'rm -rf "$planner_staging"; rm -f "$runner_output"' EXIT
"$flink_home/bin/flink" run -t local \
  -c tech.streamfusion.flink.runner.PlannerHookIntegrationJob \
  "${job_jars[0]}" | tee "$runner_output"

grep -q 'STREAMFUSION_RUNNER_INTEGRATION_OK planners=1 translations=' "$runner_output"
