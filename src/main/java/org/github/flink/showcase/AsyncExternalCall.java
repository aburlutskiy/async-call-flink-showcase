package org.github.flink.showcase;

import org.github.flink.showcase.debezium.DebeziumEnvelope;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

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

@Log4j2
@RequiredArgsConstructor
public class AsyncExternalCall extends RichAsyncFunction<DebeziumEnvelope, DebeziumEnvelope> {

    private final String externalServiceURL;
    private final Integer httpClientThreads;
    private final Integer connectionTimeoutInMinutes;
    private transient HttpClient client;
    private transient ExecutorService executor;
    private transient URI requestUri;

    @Override
    public void open(Configuration parameters) throws Exception {

        executor = Executors.newFixedThreadPool(httpClientThreads);

        client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofMinutes(connectionTimeoutInMinutes))
                .version(HttpClient.Version.HTTP_2) //keep-alive
                .executor(executor)
                .build();
        requestUri = URI.create(externalServiceURL);
    }

    @Override
    public void asyncInvoke(DebeziumEnvelope entity, final ResultFuture<DebeziumEnvelope> resultFuture) throws Exception {
        final String entityJson = entity.getPayload().getAfter();

        //API Http Request
        HttpRequest request = HttpRequest
                .newBuilder()
                .uri(requestUri)
                .header("Content-Type", "text/plain; charset=UTF-8")
                .POST(HttpRequest.BodyPublishers.ofString(entityJson))
                .build();

        client.sendAsync(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8))
                .thenAccept((response) -> {
                    entity.getPayload().setAfter(response.body());
                    resultFuture.complete(Collections.singleton(entity));
                });
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
