package suishen.elasticsearch.meta.meta.query;

import java.util.ArrayList;
import java.util.List;

/**
 * Author: Alvin Li
 * Date: 26/06/2017
 * Time: 12:02
 */
public class TermsAggMeta {
    /**
     * ES中aggregation的名字
     */
    private String name;

    /**
     * ES中的供Terms bucket使用的field
     */
    private String field;

    /**
     * 在DateHistogram Agg下附带进行的metric agg运算
     */
    private List<MetricAggMeta> metrics;

    /**
     * 字段中的值,表明是否计算agg的值
     */
    private Boolean doAgg;

    public TermsAggMeta() {
        this.doAgg = false;
        this.metrics = new ArrayList<>();
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

    public Boolean getDoAgg() {
        return doAgg;
    }

    public void setDoAgg(Boolean doAgg) {
        this.doAgg = doAgg;
    }

    public List<MetricAggMeta> getMetrics() {
        return metrics;
    }

    public void setMetrics(List<MetricAggMeta> metrics) {
        this.metrics = metrics;
    }

    public boolean addMetric(String name, MetricType type, String field) {
        return metrics.add(new MetricAggMeta(name, type, field));
    }

    @Override
    public String toString() {
        return "TermsAggMeta{" +
                "name='" + name + '\'' +
                ", field='" + field + '\'' +
                ", metrics=" + metrics +
                ", doAgg=" + doAgg +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TermsAggMeta that = (TermsAggMeta) o;

        if (name != null ? !name.equals(that.name) : that.name != null) return false;
        if (field != null ? !field.equals(that.field) : that.field != null) return false;
        if (metrics != null ? !metrics.equals(that.metrics) : that.metrics != null) return false;
        return doAgg != null ? doAgg.equals(that.doAgg) : that.doAgg == null;
    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (field != null ? field.hashCode() : 0);
        result = 31 * result + (metrics != null ? metrics.hashCode() : 0);
        result = 31 * result + (doAgg != null ? doAgg.hashCode() : 0);
        return result;
    }
}
