package org.github.flink.showcase;

import com.newrelic.api.agent.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.github.flink.showcase.debezium.DebeziumEnvelope;

import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPOutputStream;

@Log4j2
@RequiredArgsConstructor
public class AsyncExternalCall extends RichAsyncFunction<DebeziumEnvelope, DebeziumEnvelope> {

    private final String externalServiceURL;
    private final Integer httpClientThreads;
    private final Integer connectionTimeoutInSeconds;
    private transient HttpClient client;
    private transient ExecutorService executor;
    private transient URI requestUri;

    @Override
    public void open(Configuration parameters) throws Exception {

        executor = Executors.newFixedThreadPool(httpClientThreads);

        client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(connectionTimeoutInSeconds))
                .version(HttpClient.Version.HTTP_2) //keep-alive
                .executor(executor)
                .build();
        requestUri = URI.create(externalServiceURL);
    }

    @Override
    @Trace(metricName = "ExternalCallAsyncV4", dispatcher = true)
    public void asyncInvoke(DebeziumEnvelope entity, final ResultFuture<DebeziumEnvelope> resultFuture) throws Exception {
        final Transaction transaction = NewRelic.getAgent().getTransaction();
        final Token token = transaction.getToken();
//        Segment segment = transaction.startSegment("waiting-in-thread-pool");
        final String entityJson = entity.getPayload().getAfter();

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            try (GZIPOutputStream gzos = new GZIPOutputStream(baos)) {
                gzos.write(entityJson.getBytes(StandardCharsets.UTF_8));

                //API Http Request
                HttpRequest request = HttpRequest
                        .newBuilder()
                        .uri(requestUri)
                        .POST(HttpRequest.BodyPublishers.ofByteArray(baos.toByteArray()))
                        .build();

                // construct external parameters
                ExternalParameters params = HttpParameters
                        .library("HttpClient")
                        .uri(requestUri)
                        .procedure("POST")
                        .noInboundHeaders()
                        .build();
                // report the current method as doing external activity
                NewRelic.getAgent().getTracedMethod().reportAsExternal(params);

                client.sendAsync(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8))
                        //                (initiatingRequest, pushPromiseRequest, acceptor) -> {
                        //                    segment.end();
                        //                })
                        .thenAccept((response) -> {
                            entity.getPayload().setAfter(response.body());
                            token.expire();
                            resultFuture.complete(Collections.singleton(entity));
                        });

            }
        }


    }

    @Override
    public void close() throws Exception {
        if (executor != null) {
            executor.shutdown(); // Disable new tasks from being submitted
            try {
                // Wait a while for existing tasks to terminate
                if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                    executor.shutdownNow(); // Cancel currently executing tasks
                    // Wait a while for tasks to respond to being cancelled
                    if (!executor.awaitTermination(60, TimeUnit.SECONDS))
                        log.error("Pool did not terminate");
                }
            } catch (InterruptedException ie) {
                // (Re-)Cancel if current thread also interrupted
                executor.shutdownNow();
                // Preserve interrupt status
                Thread.currentThread().interrupt();
            }
        }
    }
}
