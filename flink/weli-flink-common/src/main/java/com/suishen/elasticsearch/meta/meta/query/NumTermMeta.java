package com.suishen.elasticsearch.meta.meta.query;

import com.suishen.elasticsearch.meta.meta.req.BoolType;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;

/**
 * Author: Alvin Li
 * Date: 7/12/16
 * Time: 16:43
 */
public class NumTermMeta {

    /**
     * 对应ES中的field
     */
    private String field;

    /**
     * field对应的查询值
     */
    private List<BigDecimal> values;

    private NumType type;

    private BoolType boolType;


    public NumTermMeta(String field, NumType type,BoolType boolType, BigDecimal... values) {
        this.field = field;
        this.type = type;
        this.boolType = boolType;
        this.values = Arrays.asList(values);
    }

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public NumType getType() {
        return type;
    }

    public void setType(NumType type) {
        this.type = type;
    }

    public List<BigDecimal> getValues() {
        return values;
    }

    public void setValues(List<BigDecimal> values) {
        this.values = values;
    }

    public BoolType getBoolType() {
        return boolType;
    }

    public void setBoolType(BoolType boolType) {
        this.boolType = boolType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        NumTermMeta that = (NumTermMeta) o;

        if (field != null ? !field.equals(that.field) : that.field != null) return false;
        if (values != null ? !values.equals(that.values) : that.values != null) return false;
        if (type != that.type) return false;
        return boolType == that.boolType;
    }

    @Override
    public int hashCode() {
        int result = field != null ? field.hashCode() : 0;
        result = 31 * result + (values != null ? values.hashCode() : 0);
        result = 31 * result + (type != null ? type.hashCode() : 0);
        result = 31 * result + (boolType != null ? boolType.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "NumTermMeta{" +
                "field='" + field + '\'' +
                ", values=" + values +
                ", rangeType=" + type +
                ", boolType=" + boolType +
                '}';
    }
}
