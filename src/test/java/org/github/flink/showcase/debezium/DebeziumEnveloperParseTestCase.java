package org.github.flink.showcase.debezium;

import org.github.flink.showcase.debezium.DebeziumEnvelope;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;

public class DebeziumEnveloperParseTestCase {

    @Test
    public void parseJson() throws IOException, URISyntaxException {
        final Path payloadUri = Path.of(ClassLoader.getSystemResource("example.json").toURI());
        final String payload = new String(Files.readAllBytes(payloadUri));

        final ObjectMapper jsonParser = new ObjectMapper();

        final DebeziumEnvelope envelope = jsonParser.readValue(payload, DebeziumEnvelope.class);

        Assert.assertNotNull(envelope);
        Assert.assertNotNull(envelope.getSchema());
        Assert.assertEquals("collection.Envelope", envelope.getSchema().getName());
        Assert.assertTrue(envelope.getSchema().isOptional());
        Assert.assertEquals("struct", envelope.getSchema().getType());
        Assert.assertNotNull(envelope.getSchema().getFields());
        Assert.assertEquals(8, envelope.getSchema().getFields().size());

        Assert.assertNotNull(envelope.getPayload());
        Assert.assertNull(envelope.getPayload().getPatch());
        Assert.assertNull(envelope.getPayload().getFilter());
        Assert.assertEquals("c", envelope.getPayload().getOp());
        Assert.assertNull(envelope.getPayload().getUpdateDescription());
        Assert.assertNotNull(envelope.getPayload().getAfter());
        Assert.assertNotNull(envelope.getPayload().getSource());
        Assert.assertEquals(36, envelope.getPayload().getSource().getOrd());
        Assert.assertEquals("atlas-7oo52f-shard-0", envelope.getPayload().getSource().getRs());
        Assert.assertNull(envelope.getPayload().getSource().getH());
        Assert.assertNull(envelope.getPayload().getSource().getTxnNumber());
        Assert.assertNull(envelope.getPayload().getSource().getStxnid());
        Assert.assertEquals("collection", envelope.getPayload().getSource().getCollection());
        Assert.assertEquals("1.9.2.Final", envelope.getPayload().getSource().getVersion());
        Assert.assertNull(envelope.getPayload().getSource().getLsid());
        Assert.assertNull(envelope.getPayload().getSource().getSequence());
        Assert.assertEquals("mongodb", envelope.getPayload().getSource().getConnector());
        Assert.assertEquals("mongo-pl0.PRD.dataServiceCachedb.34001b", envelope.getPayload().getSource().getName());
        Assert.assertNull(envelope.getPayload().getSource().getTord());
        Assert.assertEquals(1680557407000L, envelope.getPayload().getSource().getTs_ms());
        Assert.assertTrue(envelope.getPayload().getSource().getSnapshot());
        Assert.assertEquals("dataServiceCachedb", envelope.getPayload().getSource().getDb());
    }
}
