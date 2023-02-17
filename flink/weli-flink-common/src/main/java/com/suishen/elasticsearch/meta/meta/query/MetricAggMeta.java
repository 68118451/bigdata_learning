package com.suishen.elasticsearch.meta.meta.query;

/**
 * Author: Alvin Li
 * Date: 27/06/2017
 * Time: 16:03
 */
public class MetricAggMeta {
    private String name;

    private MetricType type;

    private String field;

    public MetricAggMeta() {
    }

    public MetricAggMeta(String name, MetricType type, String field) {
        this.name = name;
        this.type = type;
        this.field = field;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public MetricType getType() {
        return type;
    }

    public void setType(MetricType type) {
        this.type = type;
    }

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    @Override
    public String toString() {
        return "MetricAggMeta{" +
                "name='" + name + '\'' +
                ", type=" + type +
                ", field='" + field + '\'' +
                '}';
    }
}
