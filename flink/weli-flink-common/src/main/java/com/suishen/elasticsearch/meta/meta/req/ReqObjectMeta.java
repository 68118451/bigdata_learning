package com.suishen.elasticsearch.meta.meta.req;

/**
 * 表示单个值的封装类
 * Author: Alvin Li
 * Date: 7/15/16
 * Time: 12:00
 */
public class ReqObjectMeta<T>{

    /**
     * 表示该值是用来查询还是对比的
     * 注意:当前一个bean中只能有一个字段是对比类型
     */
    private ReqMetaType type;
    private T value;

    public ReqObjectMeta() {
        this.type = ReqMetaType.QUERY;
    }

    public ReqObjectMeta(T value) {
        this.value = value;
        this.type = ReqMetaType.QUERY;
    }

    public ReqObjectMeta(ReqMetaType type, T value) {
        this.type = type;
        this.value = value;
    }

    public ReqMetaType getType() {
        return type;
    }

    public void setType(ReqMetaType type) {
        this.type = type;
    }

    public T getValue() {
        return value;
    }

    public void setValue(T value) {
        this.value = value;
    }
}
