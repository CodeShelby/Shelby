package com.tiduyun.dmp.bean.pub;

import java.util.List;

public class Scheme {
    private String Type;
    private List<Object> fields;
    private Boolean optional;
    private String name;

    public String getType() {
        return Type;
    }

    public void setType(String type) {
        Type = type;
    }

    public List<Object> getFields() {
        return fields;
    }

    public void setFields(List<Object> fields) {
        this.fields = fields;
    }

    public Boolean getOptional() {
        return optional;
    }

    public void setOptional(Boolean optional) {
        this.optional = optional;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
