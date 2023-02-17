package com.suishen.elasticsearch.core;

import java.util.ArrayList;
import java.util.List;

/**
 * Author: Alvin Li
 * Date: 12/20/16
 * Time: 17:33
 */
public class ListDataResp<T> {
    private long total;

    private List<T> data;

    public ListDataResp() {
        this.total = 0;
        this.data = new ArrayList<>();
    }

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
}
