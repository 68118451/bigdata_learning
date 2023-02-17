package com.suishen.elasticsearch.meta.meta.req;


import com.google.common.collect.Lists;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * 表示多个值的封装类
 * Author: Alvin Li
 * Date: 7/15/16
 * Time: 13:47
 */
public class ReqListMeta<T>{

    /**
     * 表示该值是用来查询还是对比的
     * 注意:当前一个bean中只能有一个字段是对比类型
     */
    private ReqMetaType type;
    private List<T> value;



    public ReqListMeta() {
        this.type = ReqMetaType.QUERY;
        this.value = Lists.newArrayList();
    }

    public ReqListMeta(List<T> value) {
        this.value = value;
        this.type = ReqMetaType.QUERY;
    }

    public ReqListMeta(T... value) {
        this.value = Lists.newArrayList(value);
        this.type = ReqMetaType.QUERY;
    }

    public ReqListMeta(ReqMetaType type, List<T> value) {
        this.type = type;
        this.value = value;
    }

    public ReqListMeta(ReqMetaType type, T... value) {
        this.type = type;
        this.value = Lists.newArrayList(value);
    }

    public ReqListMeta(ReqMetaType type) {
        this.type = type;
        this.value = Lists.newArrayList();
    }

    public ReqMetaType getType() {
        return type;
    }

    public void setType(ReqMetaType type) {
        this.type = type;
    }

    public List<T> getValue() {
        return value;
    }

    public void setValue(List<T> value) {
        this.value = value;
    }

    public boolean add(T t) {
        return value.add(t);
    }

    public boolean addAll(Collection<? extends T> c) {
        return value.addAll(c);
    }

    public boolean addAll(T... c) {
        return value.addAll(Arrays.asList(c));
    }



    public int size() {
        return value.size();
    }
}
