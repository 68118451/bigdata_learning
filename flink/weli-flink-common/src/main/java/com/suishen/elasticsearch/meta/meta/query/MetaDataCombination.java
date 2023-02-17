package com.suishen.elasticsearch.meta.meta.query;

import com.suishen.elasticsearch.meta.meta.req.BoolType;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 存储指示ES查询的元数据bean
 * 是<code>ToMetaData</code>的最终结果,是<code>ToEsQuery</code>的指导数据
 * Author: Alvin Li
 * Date: 7/12/16
 * Time: 13:56
 */
public class MetaDataCombination {
    private static final Logger LOG = LoggerFactory.getLogger(MetaDataCombination.class);


    private List<StringTermMeta> strTermsQuery;

    private List<NumTermMeta> numTermsQuery;

    private List<NumRangeMeta> numRangeQuery;

    private List<DateRangeMeta> dateRangeQuery;

    private List<StringTermMeta> matchQuery;

    private List<NestedMeta> nestedQuery;

    private DateHistogramAggMeta dateHistogramAggMeta;

    private TermsAggMeta termsAggMeta;

    public List<StringTermMeta> getStrTermsQuery() {
        return strTermsQuery;
    }

    public void setStrTermsQuery(List<StringTermMeta> strTermsQuery) {
        this.strTermsQuery = strTermsQuery;
    }

    public List<NumTermMeta> getNumTermsQuery() {
        return numTermsQuery;
    }

    public void setNumTermsQuery(List<NumTermMeta> numTermsQuery) {
        this.numTermsQuery = numTermsQuery;
    }


    public List<NumRangeMeta> getNumRangeQuery() {
        return numRangeQuery;
    }

    public void setNumRangeQuery(List<NumRangeMeta> numRangeQuery) {
        this.numRangeQuery = numRangeQuery;
    }

    public List<DateRangeMeta> getDateRangeQuery() {
        return dateRangeQuery;
    }

    public void setDateRangeQuery(List<DateRangeMeta> dateRangeQuery) {
        this.dateRangeQuery = dateRangeQuery;
    }

    public List<StringTermMeta> getMatchQuery() {
        return matchQuery;
    }

    public void setMatchQuery(List<StringTermMeta> matchQuery) {
        this.matchQuery = matchQuery;
    }

    public List<NestedMeta> getNestedQuery() {
        return nestedQuery;
    }

    public void setNestedQuery(List<NestedMeta> nestedQuery) {
        this.nestedQuery = nestedQuery;
    }

    public DateHistogramAggMeta getDateHistogramAggMeta() {
        return dateHistogramAggMeta;
    }

    public void setDateHistogramAggMeta(DateHistogramAggMeta dateHistogramAggMeta) {
        this.dateHistogramAggMeta = dateHistogramAggMeta;
    }

    public TermsAggMeta getTermsAggMeta() {
        return termsAggMeta;
    }

    public void setTermsAggMeta(TermsAggMeta termsAggMeta) {
        this.termsAggMeta = termsAggMeta;
    }

    public static MetaDataCombinationBuilder newBuilder() {
        return new MetaDataCombinationBuilder();
    }

    public static MetaDataCombinationBuilder newBuilder(String nestedPath) {
        return new MetaDataCombinationBuilder(nestedPath);
    }


    @Override
    public String toString() {
        return "MetaDataCombination{" +
                "strTermsQuery=" + strTermsQuery +
                ", numTermsQuery=" + numTermsQuery +
                ", numRangeQuery=" + numRangeQuery +
                ", dateRangeQuery=" + dateRangeQuery +
                ", matchQuery=" + matchQuery +
                ", nestedQuery=" + nestedQuery +
                ", dateHistogramAggMeta=" + dateHistogramAggMeta +
                ", termsAggMeta=" + termsAggMeta +
                '}';
    }

    public static class MetaDataCombinationBuilder {

        private List<StringTermMeta> strTermsQuery;
        private List<NumTermMeta> numTermsQuery;
        private List<StringTermMeta> matchQuery;
        private Map<String, NumRangeMeta> numRangeQuery;
        private Map<String, DateRangeMeta> dateRangeQuery;
        private String fieldPrefix;
        private Map<String, NestedMeta> nestedQuery;
        private DateHistogramAggMeta dateHistogramAggMeta;
        private TermsAggMeta termsAggMeta;

        public MetaDataCombinationBuilder() {
            this("");
        }

        public MetaDataCombinationBuilder(String fieldPrefix) {
            if (StringUtils.isBlank(fieldPrefix)) {
                this.fieldPrefix = "";
            } else {
                this.fieldPrefix = fieldPrefix.trim() + ".";
            }
            this.strTermsQuery = new ArrayList<>();
            this.numTermsQuery = new ArrayList<>();
            this.numRangeQuery = new HashMap<>();
            this.dateRangeQuery = new HashMap<>();
            this.matchQuery = new ArrayList<>();
            this.nestedQuery = new HashMap<>();
            this.dateHistogramAggMeta = new DateHistogramAggMeta();
            this.termsAggMeta = new TermsAggMeta();
        }

        public MetaDataCombinationBuilder addNestedQuery(String path, BoolType boolType, MetaDataCombination innerQuery) {
            this.nestedQuery.put(path, new NestedMeta(path, boolType, innerQuery));
            return this;
        }

        public MetaDataCombinationBuilder addStrTermsQuery(String field, BoolType boolType, String... values) {
            field = fieldPrefix + field;
            this.strTermsQuery.add(new StringTermMeta(field, boolType, values));
            return this;
        }

        public MetaDataCombinationBuilder addMatchQuery(String field, BoolType boolType, String... values) {
            field = fieldPrefix + field;
            this.matchQuery.add(new StringTermMeta(field, boolType, values));
            return this;
        }


        public MetaDataCombinationBuilder addNumTermsQuery(String field, NumType numType, BoolType boolType, BigDecimal... values) {
            field = fieldPrefix + field;
            this.numTermsQuery.add(new NumTermMeta(field, numType, boolType, values));
            return this;
        }

        public MetaDataCombinationBuilder addDateRangeQuery(String field, RangeType rangeType, BoolType boolType, String value, String format) {
            if ("".equals(value)) {
                return this;
            }
            field = fieldPrefix + field;
            DateRangeMeta meta = this.dateRangeQuery.get(field);
            if (meta == null) {
                meta = new DateRangeMeta(field, boolType);
                this.dateRangeQuery.put(field, meta);
            }

            meta.setFormat(format);
            switch (rangeType) {
                case GT:
                    meta.setGt(value);
                    break;
                case GTE:
                    meta.setGte(value);
                    break;
                case LT:
                    meta.setLt(value);
                    break;
                case LTE:
                    meta.setLte(value);
                    break;
            }
            return this;
        }

        public MetaDataCombinationBuilder addNumberRangeQuery(String field, RangeType rangeType, BoolType boolType, BigDecimal value) {
            field = fieldPrefix + field;
            NumRangeMeta meta = this.numRangeQuery.get(field);
            if (meta == null) {
                meta = new NumRangeMeta(field, boolType);
                this.numRangeQuery.put(field, meta);
            }
            switch (rangeType) {
                case GT:
                    meta.setGt(value);
                    break;
                case GTE:
                    meta.setGte(value);
                    break;
                case LT:
                    meta.setLt(value);
                    break;
                case LTE:
                    meta.setLte(value);
                    break;
            }
            return this;
        }

        public MetaDataCombinationBuilder addDateHistogram(String name, String field, DateHistogramIntervalType interval, int intervalLength, int minDoc, String format) {
            this.dateHistogramAggMeta.setName(StringUtils.trimToEmpty(name));
            this.dateHistogramAggMeta.setField(StringUtils.trimToEmpty(field));
            this.dateHistogramAggMeta.setInterval(new DateHistogramInterval(String.valueOf(intervalLength) + interval.getUnit()));
            this.dateHistogramAggMeta.setMinDoc(minDoc);
            this.dateHistogramAggMeta.setFormat(format);
            return this;
        }

        public MetaDataCombinationBuilder setDateHistogramDoAgg(boolean doAgg) {
            this.dateHistogramAggMeta.setDoAgg(doAgg);
            return this;
        }

        public MetaDataCombinationBuilder setDateHistogramExtendBound(String min, String max) {
            this.dateHistogramAggMeta.setExtendBoundMin(StringUtils.trimToEmpty(min));
            this.dateHistogramAggMeta.setExtendBoundMax(StringUtils.trimToEmpty(max));
            return this;
        }

        public MetaDataCombinationBuilder addDateHistogramMetric(String name, MetricType type, String field) {
            this.dateHistogramAggMeta.addMetric(StringUtils.trimToEmpty(name), type, StringUtils.trimToEmpty(field));
            return this;
        }

        public MetaDataCombinationBuilder setTermsAggMeta(String name, String field) {
            this.termsAggMeta.setName(name);
            this.termsAggMeta.setField(field);
            return this;
        }

        public MetaDataCombinationBuilder setTermsAggDoAgg(boolean doAgg) {
            this.termsAggMeta.setDoAgg(doAgg);
            return this;
        }

        public MetaDataCombinationBuilder addTermsMetric(String name, MetricType type, String field){
            this.termsAggMeta.addMetric(StringUtils.trimToEmpty(name), type, StringUtils.trimToEmpty(field));
            return this;
        }


        public MetaDataCombination build() {
            MetaDataCombination query = new MetaDataCombination();
            query.setStrTermsQuery(this.strTermsQuery);
            query.setNumTermsQuery(this.numTermsQuery);
            query.setMatchQuery(this.matchQuery);
            query.numRangeQuery = new ArrayList<>();
            for (Map.Entry<String, NumRangeMeta> entry : this.numRangeQuery.entrySet()) {
                query.numRangeQuery.add(entry.getValue());
            }
            query.dateRangeQuery = new ArrayList<>();
            for (Map.Entry<String, DateRangeMeta> entry : this.dateRangeQuery.entrySet()) {
                query.dateRangeQuery.add(entry.getValue());
            }
            query.nestedQuery = new ArrayList<>();
            for (Map.Entry<String, NestedMeta> entry : this.nestedQuery.entrySet()) {
                query.nestedQuery.add(entry.getValue());
            }

            query.setDateHistogramAggMeta(this.dateHistogramAggMeta);
            query.setTermsAggMeta(this.termsAggMeta);
            return query;
        }


    }
}
