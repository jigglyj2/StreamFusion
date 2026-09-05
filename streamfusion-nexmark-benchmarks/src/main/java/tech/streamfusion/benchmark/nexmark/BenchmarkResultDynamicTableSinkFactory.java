package tech.streamfusion.benchmark.nexmark;

import java.util.Collections;
import java.util.Set;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;

/** Factory for the Kafka-free benchmark's exact-result sink. */
public final class BenchmarkResultDynamicTableSinkFactory implements DynamicTableSinkFactory {
    private static final ConfigOption<String> RUN_ID =
            ConfigOptions.key("run-id").stringType().noDefaultValue();

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();
        var resolvedSchema = context.getCatalogTable().getResolvedSchema();
        return new BenchmarkResultDynamicTableSink(
                resolvedSchema.toPhysicalRowDataType(),
                resolvedSchema.getPrimaryKeyIndexes(),
                helper.getOptions().get(RUN_ID));
    }

    @Override
    public String factoryIdentifier() {
        return "streamfusion-benchmark-result";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Collections.singleton(RUN_ID);
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Collections.emptySet();
    }
}
