package com.suishen.elasticsearch.meta.meta.query;


import com.suishen.elasticsearch.meta.meta.req.BoolType;

/**
 * nested 查询的元数据
 * Author: Alvin Li
 * Date: 10/04/2017
 * Time: 11:50
 */
public class NestedMeta {
    private String path;

    private BoolType boolType;

    private MetaDataCombination metaDataCombination;

    public NestedMeta() {
    }

    public NestedMeta(String path, BoolType boolType, MetaDataCombination metaDataCombination) {
        this.path = path;
        this.boolType = boolType;
        this.metaDataCombination = metaDataCombination;
    }

    public BoolType getBoolType() {
        return boolType;
    }

    public void setBoolType(BoolType boolType) {
        this.boolType = boolType;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public MetaDataCombination getMetaDataCombination() {
        return metaDataCombination;
    }

    public void setMetaDataCombination(MetaDataCombination metaDataCombination) {
        this.metaDataCombination = metaDataCombination;
    }


    @Override
    public String toString() {
        return "NestedMeta{" +
                "path='" + path + '\'' +
                ", metaDataCombination=" + metaDataCombination +
                '}';
    }
}
