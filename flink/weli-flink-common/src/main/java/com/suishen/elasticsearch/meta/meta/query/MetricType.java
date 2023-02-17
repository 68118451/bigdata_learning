package com.suishen.elasticsearch.meta.meta.query;

/**
 * Author: Alvin Li
 * Date: 7/12/16
 * Time: 13:30
 */
public enum MetricType {
    SUM("sum"),
    AVG("avg"),
    DISTINCT_COUNT("cardinality"),
    MAX("max"),
    MIN("min")
    ;

    MetricType(String type) {
        this.type = type;
    }

    private String type;

    public String getType() {
        return type;
    }
}
