package org.github.flink.showcase.debezium;

import lombok.Data;

@Data
public class Source {
    private int ord;
    private String rs;
    private String h;
    private String txnNumber;
    private String stxnid;
    private String collection;
    private String version;
    private String lsid;
    private String sequence;
    private String connector;
    private String name;
    private String tord;
    private long ts_ms;
    private Boolean snapshot;
    private String db;
}
