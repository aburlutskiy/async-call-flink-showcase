package org.github.flink.showcase;

import org.apache.commons.lang3.ObjectUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkJobRunner {

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final FlinkJobBuilder jobBuilder = FlinkJobBuilder.builder()
                .jobName(params.get("jobName", "ProcessJsonFilesFromS3ToS3"))
                .source(params.get("source", "/source"))
                .output(params.get("output", "/output"))
                .externalServiceURL(params.get("externalServiceURL", "http://localhost:8083/tokenize"))
                .connectionTimeoutInMinutes(params.getInt("connectionTimeoutInMinutes", 1))
                .asyncCapacity(params.getInt("asyncCapacity", 100))
                .httpClientThreads(params.getInt("httpClientThreads", 4))
                .build();

        System.out.println("Arguments: "+jobBuilder.toString());

        jobBuilder.use(env);
        env.execute("Flink S3-to-S3 Streaming Job");
    }

}
