package com.suishen.elasticsearch.meta.meta.req;

/**
 * Author: Alvin Li
 * Date: 12/10/16
 * Time: 14:19
 */
public enum BoolType {
    MUST("must"),
    SHOULD("should"),
    MUST_NOT("must_not"),
    FILTER("filter")
    ;
    private String type;

    BoolType(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }
}
