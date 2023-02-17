package com.suishen.elasticsearch.meta.meta.query;


import com.suishen.elasticsearch.meta.meta.req.BoolType;

/**
 * Author: Alvin Li
 * Date: 7/12/16
 * Time: 11:16
 */
public class DateRangeMeta {

    /**
     * 对应ES中的field
     */
    private String field;

    /**
     * >=
     */
    private String gte;

    /**
     * >
     */
    private String gt;

    /**
     * <=
     */
    private String lte;

    /**
     * <
     */
    private String lt;

    /**
     * 日期格式
     */
    private String format;

    private BoolType boolType;

    public DateRangeMeta(String field, BoolType boolType) {
        this.field = field;
        this.boolType = boolType;
    }


    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public String getGte() {
        return gte;
    }

    public void setGte(String gte) {
        this.gte = gte;
    }

    public String getGt() {
        return gt;
    }

    public void setGt(String gt) {
        this.gt = gt;
    }

    public String getLte() {
        return lte;
    }

    public void setLte(String lte) {
        this.lte = lte;
    }

    public String getLt() {
        return lt;
    }

    public void setLt(String lt) {
        this.lt = lt;
    }

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        if(format != null && !"".equals(format))
            this.format = format;
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

        DateRangeMeta that = (DateRangeMeta) o;

        if (field != null ? !field.equals(that.field) : that.field != null) return false;
        if (gte != null ? !gte.equals(that.gte) : that.gte != null) return false;
        if (gt != null ? !gt.equals(that.gt) : that.gt != null) return false;
        if (lte != null ? !lte.equals(that.lte) : that.lte != null) return false;
        if (lt != null ? !lt.equals(that.lt) : that.lt != null) return false;
        if (format != null ? !format.equals(that.format) : that.format != null) return false;
        return boolType == that.boolType;
    }

    @Override
    public int hashCode() {
        int result = field != null ? field.hashCode() : 0;
        result = 31 * result + (gte != null ? gte.hashCode() : 0);
        result = 31 * result + (gt != null ? gt.hashCode() : 0);
        result = 31 * result + (lte != null ? lte.hashCode() : 0);
        result = 31 * result + (lt != null ? lt.hashCode() : 0);
        result = 31 * result + (format != null ? format.hashCode() : 0);
        result = 31 * result + (boolType != null ? boolType.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "DateRangeMeta{" +
                "field='" + field + '\'' +
                ", gte='" + gte + '\'' +
                ", gt='" + gt + '\'' +
                ", lte='" + lte + '\'' +
                ", lt='" + lt + '\'' +
                ", format='" + format + '\'' +
                ", boolType=" + boolType +
                '}';
    }
}
