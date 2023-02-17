package com.suishen.elasticsearch.meta.meta.query;

/**
 * Author: Alvin Li
 * Date: 12/10/16
 * Time: 15:43
 */
public class SortOrderMeta {
    private String field;

    private SortOrderType order;

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public SortOrderType getOrder() {
        return order;
    }

    public void setOrder(SortOrderType order) {
        this.order = order;
    }
}
