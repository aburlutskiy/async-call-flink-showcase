package org.github.flink.showcase.debezium;

import lombok.Data;

@Data
public class DebeziumEnvelope {
    private Schema schema;
    private Payload payload;
}
