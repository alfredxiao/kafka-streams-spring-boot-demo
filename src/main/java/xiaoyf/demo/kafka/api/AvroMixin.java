package xiaoyf.demo.kafka.api;

import com.fasterxml.jackson.annotation.JsonIgnore;

public abstract class AvroMixin {

    @JsonIgnore
    abstract void getClassSchema();

    @JsonIgnore
    abstract void getSpecificData();

    @JsonIgnore
    abstract void get();

    @JsonIgnore
    abstract void getSchema();
}
