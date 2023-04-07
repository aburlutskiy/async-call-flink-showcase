package org.github.flink.showcase.debezium;

import lombok.Data;

import java.util.List;

@Data
public class Items {
    private boolean optional;
    private String type;
    private String name;
    private List<Field> fields;
}
