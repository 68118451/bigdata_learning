package com.suishen.elasticsearch.meta.meta.query;

import org.apache.commons.collections4.CollectionUtils;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;

import java.util.ArrayList;
import java.util.List;

/**
 * Author: Alvin Li
 * Date: 7/12/16
 * Time: 11:23
 */
public class DateHistogramAggMeta {

    /**
     * ES中aggregation的名字
     */
    private String name;

    /**
     * ES中的供DateHistogram bucket使用的field
     */
    private String field;

    /**
     * 字段中的值,表明是否计算agg的值
     *
     */
    private Boolean doAgg;


    /**
     * DateHistogram Agg 的统计时间间隔
     */
    private DateHistogramInterval interval;


    /**
     * DateHistogram Agg 后返回统计的最小文档数。设置为0可以将统计为0的区间结果也返回
     */
    private int minDoc;

    /**
     * 日期格式
     */
    private String format;


    /**
     * 在DateHistogram Agg下附带进行的metric agg运算
     */
    private List<MetricAggMeta> metrics;

    private String extendBoundMin;

    private String extendBoundMax;

    public DateHistogramAggMeta() {
        this.metrics = new ArrayList<>();
        this.doAgg = false;
    }



    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public DateHistogramInterval getInterval() {
        return interval;
    }

    public void setInterval(DateHistogramInterval interval) {
        this.interval = interval;
    }

    public int getMinDoc() {
        return minDoc;
    }

    public void setMinDoc(int minDoc) {
        this.minDoc = minDoc;
    }

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }


    public List<MetricAggMeta> getMetrics() {
        return metrics;
    }

    public void setMetrics(List<MetricAggMeta> metrics) {
        this.metrics = metrics;
    }

    public boolean addMetric(String name, MetricType type, String field) {
        return metrics.add(new MetricAggMeta(name, type,field));
    }
    public Boolean getDoAgg() {
        return doAgg;
    }

    public void setDoAgg(Boolean doAgg) {
        this.doAgg = doAgg;
    }

    public String getExtendBoundMin() {
        return extendBoundMin;
    }

    public void setExtendBoundMin(String extendBoundMin) {
        this.extendBoundMin = extendBoundMin;
    }

    public String getExtendBoundMax() {
        return extendBoundMax;
    }

    public void setExtendBoundMax(String extendBoundMax) {
        this.extendBoundMax = extendBoundMax;
    }

    @Override
    public String toString() {
        return "DateHistogramAggMeta{" +
                "name='" + name + '\'' +
                ", field='" + field + '\'' +
                ", doAgg=" + doAgg +
                ", interval=" + interval +
                ", minDoc=" + minDoc +
                ", format='" + format + '\'' +
                ", metrics=" + metrics +
                ", extendBoundMin='" + extendBoundMin + '\'' +
                ", extendBoundMax='" + extendBoundMax + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DateHistogramAggMeta that = (DateHistogramAggMeta) o;

        if (minDoc != that.minDoc) return false;
        if (name != null ? !name.equals(that.name) : that.name != null) return false;
        if (field != null ? !field.equals(that.field) : that.field != null) return false;
        if (doAgg != null ? !doAgg.equals(that.doAgg) : that.doAgg != null) return false;
        if (interval != null ? !interval.equals(that.interval) : that.interval != null) return false;
        if (format != null ? !format.equals(that.format) : that.format != null) return false;
        if (metrics != null ? !CollectionUtils.isEqualCollection(metrics,that.metrics): that.metrics != null) return false;
        if (extendBoundMin != null ? !extendBoundMin.equals(that.extendBoundMin) : that.extendBoundMin != null)
            return false;
        return extendBoundMax != null ? extendBoundMax.equals(that.extendBoundMax) : that.extendBoundMax == null;
    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (field != null ? field.hashCode() : 0);
        result = 31 * result + (doAgg != null ? doAgg.hashCode() : 0);
        result = 31 * result + (interval != null ? interval.hashCode() : 0);
        result = 31 * result + minDoc;
        result = 31 * result + (format != null ? format.hashCode() : 0);
        result = 31 * result + (metrics != null ? metrics.hashCode() : 0);
        result = 31 * result + (extendBoundMin != null ? extendBoundMin.hashCode() : 0);
        result = 31 * result + (extendBoundMax != null ? extendBoundMax.hashCode() : 0);
        return result;
    }
}
