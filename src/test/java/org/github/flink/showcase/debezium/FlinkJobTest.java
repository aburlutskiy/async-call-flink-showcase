package org.github.flink.showcase.debezium;

import org.github.flink.showcase.FlinkJobBuilder;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.ClassRule;

public class FlinkJobTest {

    @ClassRule
    public static MiniClusterWithClientResource flink =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setConfiguration(getFlinkConfiguration())
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(2)
                            .build());

    public static Configuration getFlinkConfiguration() {
        Configuration flinkConfig = new Configuration();
        flinkConfig.set(TaskManagerOptions.MANAGED_MEMORY_SIZE, MemorySize.parse("1024m"));
        return flinkConfig;
    }

//    @Test
    public void doFlink() throws Exception {
        StreamExecutionEnvironment env = TestStreamEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        FlinkJobBuilder.builder().jobName("ProcessFromS3ToS3").source("./source").output("./output").build().use(env);
        env.execute("Flink S3-to-S3 Streaming Job");

    }
}
