package org.github.flink.showcase.debezium;

import lombok.Data;

import java.util.List;

@Data
public class Schema {
    private String name;
    private boolean optional;
    private String type;
    private List<Field> fields;
}
