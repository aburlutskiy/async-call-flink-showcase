package org.github.flink.showcase;

import com.newrelic.api.agent.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.hc.client5.http.async.methods.SimpleHttpRequest;
import org.apache.hc.client5.http.async.methods.SimpleHttpResponse;
import org.apache.hc.client5.http.async.methods.SimpleRequestBuilder;
import org.apache.hc.client5.http.async.methods.SimpleResponseConsumer;
import org.apache.hc.client5.http.impl.async.CloseableHttpAsyncClient;
import org.apache.hc.client5.http.impl.async.HttpAsyncClients;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManager;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManagerBuilder;
import org.apache.hc.core5.concurrent.FutureCallback;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.Method;
import org.apache.hc.core5.http.message.StatusLine;
import org.apache.hc.core5.http.nio.StreamChannel;
import org.apache.hc.core5.http.nio.entity.StringAsyncEntityProducer;
import org.apache.hc.core5.http.nio.support.BasicRequestProducer;
import org.apache.hc.core5.io.CloseMode;
import org.apache.hc.core5.pool.PoolConcurrencyPolicy;
import org.apache.hc.core5.reactor.IOReactorConfig;
import org.apache.hc.core5.util.TimeValue;
import org.apache.hc.core5.util.Timeout;
import org.github.flink.showcase.debezium.DebeziumEnvelope;

import java.io.IOException;
import java.net.URI;
import java.nio.CharBuffer;
import java.util.Collections;

@Log4j2
@RequiredArgsConstructor
public class AsyncExternalCallByApache extends RichAsyncFunction<DebeziumEnvelope, DebeziumEnvelope> {

  private final String externalServiceURL;

  private final Integer maxHttpClientConnections;

  private final Integer connectionTimeoutInSeconds;

  private transient CloseableHttpAsyncClient client;

  private transient URI requestUri;

  private transient ExternalParameters externalNewRelicParameters;

  @Override
  public void open(Configuration parameters) throws Exception {

    final IOReactorConfig ioReactorConfig = IOReactorConfig.custom()
            .setSoTimeout(Timeout.ofSeconds(connectionTimeoutInSeconds))
            .build();

    final PoolingAsyncClientConnectionManager connectionManager = PoolingAsyncClientConnectionManagerBuilder.create()
            .setMaxConnTotal(maxHttpClientConnections)
            .setPoolConcurrencyPolicy(PoolConcurrencyPolicy.LAX)
            .build();

    client = HttpAsyncClients.custom()
            .setConnectionManager(connectionManager)
            .setIOReactorConfig(ioReactorConfig)
            .evictExpiredConnections()
            .evictIdleConnections(TimeValue.ofMinutes(10))
            .build();

    client.start();

    requestUri = URI.create(externalServiceURL);

    // construct external parameters
    externalNewRelicParameters = HttpParameters
            .library("Apache HttpClient")
            .uri(requestUri)
            .procedure("POST")
            .noInboundHeaders()
            .build();
  }

  @Override
  @Trace(metricName = "ExternalCallAsync", dispatcher = true)
  public void asyncInvoke(DebeziumEnvelope entity, final ResultFuture<DebeziumEnvelope> resultFuture) throws Exception {
    final Transaction transaction = NewRelic.getAgent().getTransaction();

    final Token token = transaction.getToken();
    Segment segment = transaction.startSegment("waiting-in-thread-pool");

    // report the current method as doing external activity
    NewRelic.getAgent().getTracedMethod().reportAsExternal(externalNewRelicParameters);

    final SimpleHttpRequest request = SimpleRequestBuilder.create(Method.POST)
            .setUri(requestUri)
            .setBody(entity.getPayload().getAfter(), ContentType.APPLICATION_JSON)
            .build();

    log.debug("Executing request: {}", request);

    //execute http request
    client.execute(
            new BasicRequestProducer(request,
                    new StringAsyncEntityProducer(request.getBody().getBodyText(), request.getBody().getContentType()) {
                      @Override
                      protected void produceData(StreamChannel<CharBuffer> channel) throws IOException {
                        segment.end();
                        super.produceData(channel);
                      }
                    }),
            SimpleResponseConsumer.create(),
            new FutureCallback<>() {

              @Override
              public void completed(final SimpleHttpResponse response) {
                log.debug("{} -> {}", request, new StatusLine(response));

                entity.getPayload().setAfter(response.getBodyText());
                token.expire();
                resultFuture.complete(Collections.singleton(entity));
              }

              @Override
              public void failed(final Exception ex) {
                log.error("{} -> {}", request, ex.getMessage(), ex);
                token.expire();
              }

              @Override
              public void cancelled() {
                log.warn("{} has cancelled", request);
                token.expire();
              }

            });

  }

  @Override
  public void close() throws Exception {
    if (client != null) {
      try {
        log.debug("Shutting down of Apache Http client");
        client.close(CloseMode.GRACEFUL);
      } finally {
        client.close(CloseMode.IMMEDIATE);
      }
    }
  }

}
