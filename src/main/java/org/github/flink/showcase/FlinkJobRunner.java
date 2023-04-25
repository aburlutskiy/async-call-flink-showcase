package org.github.flink.showcase;

import lombok.extern.log4j.Log4j2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

@Log4j2
public class FlinkJobRunner {

  public static void main(String[] args) throws Exception {

    final ParameterTool params = ParameterTool.fromArgs(args);

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    final String jobName = params.get("jobName", "Flink S3-to-S3 Streaming Job V2");
    final int asyncCapacity = params.getInt("asyncCapacity", 100);
    final int connectionTimeoutInSeconds = params.getInt("connectionTimeoutInSeconds", 60);
    final FlinkJobBuilder jobBuilder = FlinkJobBuilder.builder()
            .jobName(jobName)
            .source(params.get("source", "/source"))
            .output(params.get("output", "/output"))
            .externalServiceURL(params.get("externalServiceURL", "http://localhost:8083/tokenize"))
            .connectionTimeoutInSeconds(connectionTimeoutInSeconds)
            .maxHttpClientConnections(asyncCapacity * 2)
            .asyncCapacity(asyncCapacity)
            .build();

    log.debug("Arguments: " + jobBuilder.toString());

    jobBuilder.use(env);
    env.execute(jobName);
  }

}
