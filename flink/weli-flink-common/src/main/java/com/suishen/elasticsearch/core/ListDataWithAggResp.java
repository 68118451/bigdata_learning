package com.suishen.elasticsearch.core;

import org.elasticsearch.search.aggregations.Aggregations;

import java.util.List;

/**
 * Author: Alvin Li
 * Date: 15/06/2017
 * Time: 17:26
 */
public class ListDataWithAggResp<T> {
    private long total;

    private List<T> data;

    private Aggregations aggs;

    public long getTotal() {
        return total;
    }

    public void setTotal(long total) {
        this.total = total;
    }

    public List<T> getData() {
        return data;
    }

    public void setData(List<T> data) {
        this.data = data;
    }

    public Aggregations getAggs() {
        return aggs;
    }

    public void setAggs(Aggregations aggs) {
        this.aggs = aggs;
    }
}
