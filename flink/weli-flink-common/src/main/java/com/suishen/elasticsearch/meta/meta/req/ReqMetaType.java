package com.suishen.elasticsearch.meta.meta.req;

/**
 * 表示字段值的作用,是用来查询还是对比的
 * Author: Alvin Li
 * Date: 7/15/16
 * Time: 12:01
 */
public enum ReqMetaType {
    QUERY("query"),
    COMPARE("compare")
    ;

    private String type;

    ReqMetaType(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }
}
