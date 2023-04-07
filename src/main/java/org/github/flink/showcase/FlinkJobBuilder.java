package org.github.flink.showcase;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.github.flink.showcase.debezium.DebeziumEnvelope;
import lombok.Builder;
import lombok.Data;
import lombok.ToString;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipParameters;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.AsyncRetryStrategy;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.util.retryable.AsyncRetryStrategies;
import org.apache.flink.streaming.util.retryable.RetryPredicates;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.zip.Deflater;

@Builder
@ToString
public class FlinkJobBuilder implements Serializable {

    private final String jobName;
    private final String source;
    private final String output;
    private final String externalServiceURL;
    private final Integer httpClientThreads;
    private final Integer connectionTimeoutInMinutes;
    private final Integer asyncCapacity;

    private DataStream<String> createSourceFromStaticConfig(StreamExecutionEnvironment env) {
        final FileSource<String> fileSource =
                FileSource.forRecordStreamFormat(new TextLineInputFormat(), new Path(source))
                        .monitorContinuously(Duration.ofSeconds(5))
                        .build();
        final WatermarkStrategy<String> strategy = WatermarkStrategy.<String>forMonotonousTimestamps().withIdleness(Duration.ofSeconds(60));
        return env.fromSource(fileSource, strategy, "FileSource").uid(String.format("%s-source-operator", jobName));
    }

    private FileSink<String> createS3SinkFromStaticConfig() {

        return FileSink
                .forRowFormat(new Path(output), new Encoder<String>() {
                    @Override
                    public void encode(String record, OutputStream stream)
                            throws IOException {
                        GzipParameters params = new GzipParameters();
                        params.setCompressionLevel(Deflater.BEST_COMPRESSION);
                        GzipCompressorOutputStream out = new GzipCompressorOutputStream(stream, params);
                        out.write(record.getBytes("UTF-8"));
                        out.finish();
                    }

                })
                .withBucketAssigner(new DateTimeBucketAssigner<String>("'year='yyyy'/month='MM'/day='dd'/hour='HH'/'"))
//                .enableCompact(
//                        FileCompactStrategy.Builder.newBuilder()
//                                .setSizeThreshold(1024)
//                                .enableCompactionOnCheckpoint(5)
//                                .build(),
//                        new ConcatFileCompactor())
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withInactivityInterval(Duration.ofMinutes(1))
                                .withRolloverInterval(Duration.ofMinutes(1))
                                .withMaxPartSize(new MemorySize(1024 * 1024))
                                .build())
                .withOutputFileConfig(OutputFileConfig.builder()
                        .withPartSuffix(".json.gz")
                        .build())
                .build();
    }

    public void use(StreamExecutionEnvironment env) {
        final ObjectMapper jsonParser = new ObjectMapper();
        final DataStream<String> sourceFiles = createSourceFromStaticConfig(env);

        final SingleOutputStreamOperator<DebeziumEnvelope> debeziumEnvelopeStream = sourceFiles
                .map(value -> {
                    // Parse the JSON
                    return jsonParser.readValue(value, DebeziumEnvelope.class);
                })
                .returns(DebeziumEnvelope.class)
                .map(debeziumEnvelope -> {
                    System.out.println("Debezium schema before external call: " + debeziumEnvelope.getPayload().getSource().getConnector());
                    return debeziumEnvelope;
                });


        // or apply the async I/O transformation with retry
        // create an async retry strategy via utility class or a user defined strategy
        AsyncRetryStrategy<DebeziumEnvelope> asyncRetryStrategy =
                new AsyncRetryStrategies.ExponentialBackoffDelayRetryStrategyBuilder<DebeziumEnvelope>(
                        Integer.MAX_VALUE,
                        1000L,
                        5 * 60 * 1000L,
                        1.2
                )
                        .ifException(RetryPredicates.HAS_EXCEPTION_PREDICATE)
                        .ifResult(RetryPredicates.EMPTY_RESULT_PREDICATE)
                        .build();

        // apply the async I/O transformation with retry
        AsyncDataStream.orderedWaitWithRetry(debeziumEnvelopeStream,
                        new AsyncExternalCall(externalServiceURL,
                                httpClientThreads,
                                connectionTimeoutInMinutes),
                        connectionTimeoutInMinutes * 2,
                        TimeUnit.MINUTES,
                        asyncCapacity,
                        asyncRetryStrategy)
                .name("ExternalCall")
                .uid(String.format("%s-external-call", jobName))
                .map(debeziumEnvelope -> {
                    System.out.println("Debezium schema after external call: " + debeziumEnvelope.getPayload().getSource().getConnector());
                    return debeziumEnvelope;
                })
                .map(jsonParser::writeValueAsString)
                .sinkTo(createS3SinkFromStaticConfig())
                .name("FileSink")
                .uid(String.format("%s-sink-operator", jobName));

        configureEnvironment(env);
    }

    private void configureEnvironment(StreamExecutionEnvironment env) {
        // start a checkpoint every 10000 ms
        env.enableCheckpointing(10000);
        env.getConfig().setUseSnapshotCompression(true);

        // advanced options:
        final CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setAlignedCheckpointTimeout(Duration.ofMinutes(1));
        // set mode to exactly-once (this is the default)
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // make sure 500 ms of progress happen between checkpoints
        checkpointConfig.setMinPauseBetweenCheckpoints(500);

        // checkpoints have to complete within one minute, or are discarded
        checkpointConfig.setCheckpointTimeout(60000);
        // only 10 consecutive checkpoint failures are tolerated
        checkpointConfig.setTolerableCheckpointFailureNumber(10);
        // allow only one checkpoint to be in progress at the same time
        checkpointConfig.setMaxConcurrentCheckpoints(1);
        // enable externalized checkpoints which are retained
        // after job cancellation
        checkpointConfig.setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // enables the unaligned checkpoints
        checkpointConfig.enableUnalignedCheckpoints();

        env.setMaxParallelism(Short.MAX_VALUE);
    }

}
