package com.suishen.elasticsearch.meta.meta.query;


import com.suishen.elasticsearch.meta.meta.req.BoolType;

import java.math.BigDecimal;

/**
 * Author: Alvin Li
 * Date: 7/12/16
 * Time: 11:18
 */
public class NumRangeMeta {

    /**
     * 对应ES中的field
     */
    private String field;

    /**
     * >=
     */
    private BigDecimal gte;

    /**
     * >
     */
    private BigDecimal gt;

    /**
     * <=
     */
    private BigDecimal lte;

    /**
     * <
     */
    private BigDecimal lt;

    private BoolType boolType;


    public NumRangeMeta(String field,BoolType boolType) {
        this.field = field;
        this.boolType = boolType;
    }

    public NumRangeMeta(String field, BigDecimal gte, BigDecimal gt, BigDecimal lte, BigDecimal lt) {
        this.field = field;
        this.gte = gte;
        this.gt = gt;
        this.lte = lte;
        this.lt = lt;
    }

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public BigDecimal getGte() {
        return gte;
    }

    public void setGte(BigDecimal gte) {
        this.gte = gte;
    }

    public BigDecimal getGt() {
        return gt;
    }

    public void setGt(BigDecimal gt) {
        this.gt = gt;
    }

    public BigDecimal getLte() {
        return lte;
    }

    public void setLte(BigDecimal lte) {
        this.lte = lte;
    }

    public BigDecimal getLt() {
        return lt;
    }

    public void setLt(BigDecimal lt) {
        this.lt = lt;
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

        NumRangeMeta that = (NumRangeMeta) o;

        if (field != null ? !field.equals(that.field) : that.field != null) return false;
        if (gte != null ? !gte.equals(that.gte) : that.gte != null) return false;
        if (gt != null ? !gt.equals(that.gt) : that.gt != null) return false;
        if (lte != null ? !lte.equals(that.lte) : that.lte != null) return false;
        if (lt != null ? !lt.equals(that.lt) : that.lt != null) return false;
        return boolType == that.boolType;
    }

    @Override
    public int hashCode() {
        int result = field != null ? field.hashCode() : 0;
        result = 31 * result + (gte != null ? gte.hashCode() : 0);
        result = 31 * result + (gt != null ? gt.hashCode() : 0);
        result = 31 * result + (lte != null ? lte.hashCode() : 0);
        result = 31 * result + (lt != null ? lt.hashCode() : 0);
        result = 31 * result + (boolType != null ? boolType.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "NumRangeMeta{" +
                "field='" + field + '\'' +
                ", gte=" + gte +
                ", gt=" + gt +
                ", lte=" + lte +
                ", lt=" + lt +
                ", boolType=" + boolType +
                '}';
    }
}
