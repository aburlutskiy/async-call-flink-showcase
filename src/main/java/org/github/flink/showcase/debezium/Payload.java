package org.github.flink.showcase.debezium;

import lombok.Data;

@Data
public class Payload {
    private String patch;
    private String filter;
    private String op;
    private Object updateDescription;
    private String after;
    private Source source;
    private long ts_ms;
    private String transaction;
}
