package org.github.flink.showcase.debezium;

import lombok.Data;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

@Data
public class Field {
    private String field;
    private String name;
    private boolean optional;
    private String type;
    private int version;
    private List<Field> fields;
    private Items items;
    @JsonProperty("default")
    private String mydefault;
    private Parameters parameters;
}
