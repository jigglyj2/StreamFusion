# StreamFusion

StreamFusion is an Apache Flink SQL accelerator built on Apache DataFusion. Flink remains
responsible for planning, distribution, checkpoints, recovery, and lifecycle management; supported
operators will progressively be replaced at the execution boundary.

## Flink SQL compatibility harness

The first integration seam is a small patch to Flink's planner factory discovery. It allows a test
or deployment to select `tech.streamfusion.flink.StreamFusionPlannerFactory` with the
`tech.streamfusion.flink.planner.factory` system property. Without that property, Flink follows its
normal planner discovery path unchanged.

The SQL parity test executes the same ordered batch query once with unmodified Flink planning and
once through the StreamFusion planner. Results are encoded into a length-prefixed UTF-8 byte stream
and compared exactly. It also asserts that StreamFusion created and translated the accelerated
plan, preventing a silently inactive test configuration.

Run the harness against a patched Flink 2.3.0 checkout:

```bash
cd ../flink
git checkout release-2.3.0
git apply ../StreamFusion/dev/flink/2.3.0-planner-factory.patch
./mvnw install -DskipTests -Dfast -Pskip-webui-build -T1C \
  -pl flink-table/flink-table-api-java,flink-table/flink-table-planner -am
cd ../StreamFusion
mvn verify
```

CI performs these steps from clean checkouts, so a stale patch, a changed Flink integration point,
an inactive StreamFusion planner, or a result mismatch fails the build.
