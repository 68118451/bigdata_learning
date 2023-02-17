package com.suishen.elasticsearch.meta.meta.util;

import com.suishen.elasticsearch.meta.meta.query.*;
import com.suishen.elasticsearch.meta.meta.req.BoolType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.math.BigDecimal;
import java.util.List;

/**
 * 利用元数据bean指示ES Query的生成
 * <p>
 * Author: Alvin Li
 * Date: 7/13/16
 * Time: 14:15
 */
public class ToEsQuery {
    private static final Logger LOG = LoggerFactory.getLogger(ToEsQuery.class);


    /**
     * 根据元数据生成Bool query
     *
     * @param metadata
     * @return
     */
    public static QueryBuilder toBoolQueryBuilder(MetaDataCombination metadata) {
        return toBoolQueryBuilderInner(metadata);
    }

    /**
     * 根据元数据生成包含nested的bool query
     *
     * @param metadata
     * @return
     */
    public static QueryBuilder toBoolWithNestedQueryBuilder(MetaDataCombination metadata) {
        BoolQueryBuilder boolQueryBuilder = toBoolQueryBuilderInner(metadata);
        for (NestedMeta nested : metadata.getNestedQuery()) {
            if (nested.getBoolType() == BoolType.FILTER) {
                boolQueryBuilder.filter(QueryBuilders.nestedQuery(nested.getPath(), toBoolQueryBuilder(nested.getMetaDataCombination())));
            } else if (nested.getBoolType() == BoolType.MUST) {
                boolQueryBuilder.must(QueryBuilders.nestedQuery(nested.getPath(), toBoolQueryBuilder(nested.getMetaDataCombination())));
            } else if (nested.getBoolType() == BoolType.SHOULD) {
                boolQueryBuilder.should(QueryBuilders.nestedQuery(nested.getPath(), toBoolQueryBuilder(nested.getMetaDataCombination())));
            } else {
                boolQueryBuilder.mustNot(QueryBuilders.nestedQuery(nested.getPath(), toBoolQueryBuilder(nested.getMetaDataCombination())));
            }
        }
        return boolQueryBuilder;
    }

    private static BoolQueryBuilder toBoolQueryBuilderInner(MetaDataCombination metadata) {
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();

        //match
        for (StringTermMeta stm : metadata.getMatchQuery()) {
            List<String> values = stm.getValues();
            int size = values.size();
            if (size == 1) {
                if (stm.getBoolType() == BoolType.FILTER) {
                    boolQueryBuilder.filter(QueryBuilders.matchQuery(stm.getField(), values.get(0)));
                } else if (stm.getBoolType() == BoolType.MUST) {
                    boolQueryBuilder.must(QueryBuilders.matchQuery(stm.getField(), values.get(0)));
                } else if (stm.getBoolType() == BoolType.SHOULD) {
                    boolQueryBuilder.should(QueryBuilders.matchQuery(stm.getField(), values.get(0)));
                } else {
                    boolQueryBuilder.mustNot(QueryBuilders.matchQuery(stm.getField(), values.get(0)));
                }

            } else if (size > 1) {
                if (stm.getBoolType() == BoolType.FILTER) {
                    boolQueryBuilder.filter(QueryBuilders.matchQuery(stm.getField(), values.toArray(new String[size])));
                } else if (stm.getBoolType() == BoolType.MUST) {
                    boolQueryBuilder.must(QueryBuilders.matchQuery(stm.getField(), values.toArray(new String[size])));
                } else if (stm.getBoolType() == BoolType.SHOULD) {
                    boolQueryBuilder.should(QueryBuilders.matchQuery(stm.getField(), values.toArray(new String[size])));
                } else {
                    boolQueryBuilder.mustNot(QueryBuilders.matchQuery(stm.getField(), values.toArray(new String[size])));
                }

            }
        }

        //Query
        for (StringTermMeta stm : metadata.getStrTermsQuery()) {
            List<String> values = stm.getValues();
            int size = values.size();
            if (size == 1) {
                if (stm.getBoolType() == BoolType.FILTER) {
                    boolQueryBuilder.filter(QueryBuilders.termQuery(stm.getField(), values.get(0)));
                } else if (stm.getBoolType() == BoolType.MUST) {
                    boolQueryBuilder.must(QueryBuilders.termQuery(stm.getField(), values.get(0)));
                } else if (stm.getBoolType() == BoolType.SHOULD) {
                    boolQueryBuilder.should(QueryBuilders.termQuery(stm.getField(), values.get(0)));
                } else {
                    boolQueryBuilder.mustNot(QueryBuilders.termQuery(stm.getField(), values.get(0)));
                }

            } else if (size > 1) {
                if (stm.getBoolType() == BoolType.FILTER) {
                    boolQueryBuilder.filter(QueryBuilders.termsQuery(stm.getField(), values.toArray(new String[size])));
                } else if (stm.getBoolType() == BoolType.MUST) {
                    boolQueryBuilder.must(QueryBuilders.termsQuery(stm.getField(), values.toArray(new String[size])));
                } else if (stm.getBoolType() == BoolType.SHOULD) {
                    boolQueryBuilder.should(QueryBuilders.termsQuery(stm.getField(), values.toArray(new String[size])));
                } else {
                    boolQueryBuilder.mustNot(QueryBuilders.termsQuery(stm.getField(), values.toArray(new String[size])));
                }

            }
        }

        for (NumTermMeta ntm : metadata.getNumTermsQuery()) {
            List<BigDecimal> values = ntm.getValues();
            int size = values.size();
            if (size == 1) {
                if (ntm.getBoolType() == BoolType.FILTER) {
                    boolQueryBuilder.filter(QueryBuilders.termQuery(ntm.getField(), values.get(0)));
                } else if (ntm.getBoolType() == BoolType.MUST) {
                    boolQueryBuilder.must(QueryBuilders.termQuery(ntm.getField(), values.get(0)));
                } else if (ntm.getBoolType() == BoolType.SHOULD) {
                    boolQueryBuilder.should(QueryBuilders.termQuery(ntm.getField(), values.get(0)));
                } else {
                    boolQueryBuilder.mustNot(QueryBuilders.termQuery(ntm.getField(), values.get(0)));
                }

            } else if (size > 1) {
                if (ntm.getBoolType() == BoolType.FILTER) {
                    boolQueryBuilder.filter(QueryBuilders.termsQuery(ntm.getField(), values.toArray(new BigDecimal[size])));
                } else if (ntm.getBoolType() == BoolType.MUST) {
                    boolQueryBuilder.must(QueryBuilders.termsQuery(ntm.getField(), values.toArray(new BigDecimal[size])));
                } else if (ntm.getBoolType() == BoolType.SHOULD) {
                    boolQueryBuilder.should(QueryBuilders.termsQuery(ntm.getField(), values.toArray(new BigDecimal[size])));
                } else {
                    boolQueryBuilder.mustNot(QueryBuilders.termsQuery(ntm.getField(), values.toArray(new BigDecimal[size])));
                }
            }
        }

        for (DateRangeMeta drm : metadata.getDateRangeQuery()) {
            RangeQueryBuilder rqb = QueryBuilders.rangeQuery(drm.getField()).format(drm.getFormat());
            if (drm.getGt() != null) {
                rqb.gt(drm.getGt());
            }
            if (drm.getGte() != null) {
                rqb.gte(drm.getGte());
            }
            if (drm.getLt() != null) {
                rqb.lt(drm.getLt());
            }
            if (drm.getLte() != null) {
                rqb.lte(drm.getLte());
            }
            if (drm.getBoolType() == BoolType.FILTER) {
                boolQueryBuilder.filter(rqb);
            } else if (drm.getBoolType() == BoolType.MUST) {
                boolQueryBuilder.must(rqb);
            } else if (drm.getBoolType() == BoolType.SHOULD) {
                boolQueryBuilder.should(rqb);
            } else {
                boolQueryBuilder.mustNot(rqb);
            }

        }

        for (NumRangeMeta nrm : metadata.getNumRangeQuery()) {
            RangeQueryBuilder rqb = QueryBuilders.rangeQuery(nrm.getField());
            if (nrm.getGt() != null) {
                rqb.gt(nrm.getGt());
            }
            if (nrm.getGte() != null) {
                rqb.gte(nrm.getGte());
            }
            if (nrm.getLt() != null) {
                rqb.lt(nrm.getLt());
            }
            if (nrm.getLte() != null) {
                rqb.lte(nrm.getLte());
            }
            if (nrm.getBoolType() == BoolType.FILTER) {
                boolQueryBuilder.filter(rqb);
            } else if (nrm.getBoolType() == BoolType.MUST) {
                boolQueryBuilder.must(rqb);
            } else if (nrm.getBoolType() == BoolType.SHOULD) {
                boolQueryBuilder.should(rqb);
            } else {
                boolQueryBuilder.mustNot(rqb);
            }
        }
        return boolQueryBuilder;
    }

    /**
     * 生成DateHistogram的聚合
     *
     * @param metadata
     * @return
     */
    public static DateHistogramBuilder toDateHistogramAggBuilder(MetaDataCombination metadata) {
        DateHistogramAggMeta dha = metadata.getDateHistogramAggMeta();
        if (!dha.getDoAgg()) {
            return null;
        }
        DateHistogramBuilder dhBuilder = AggregationBuilders.dateHistogram(dha.getName())
                .field(dha.getField())
                .interval(dha.getInterval())
                .minDocCount(dha.getMinDoc())
                .format(dha.getFormat());
        for (MetricAggMeta metric : dha.getMetrics()) {
            switch (metric.getType()) {
                case SUM:
                    dhBuilder.subAggregation(AggregationBuilders.sum(metric.getName()).field(metric.getField()));
                    break;
                case AVG:
                    dhBuilder.subAggregation(AggregationBuilders.avg(metric.getName()).field(metric.getField()));
                    break;
                case DISTINCT_COUNT:
                    dhBuilder.subAggregation(AggregationBuilders.cardinality(metric.getName()).field(metric.getField()));
                    break;
                case MAX:
                    dhBuilder.subAggregation(AggregationBuilders.max(metric.getName()).field(metric.getField()));
                    break;
                case MIN:
                    dhBuilder.subAggregation(AggregationBuilders.min(metric.getName()).field(metric.getField()));
                    break;
            }
        }

        return dhBuilder;
    }

    public static TermsBuilder toTermsAggBuilder(MetaDataCombination metadata) {
        TermsAggMeta tam = metadata.getTermsAggMeta();
        if (!tam.getDoAgg()) {
            return null;
        }
        TermsBuilder termsBuilder = AggregationBuilders.terms(tam.getName()).field(tam.getField());
        for (MetricAggMeta metric : tam.getMetrics()) {
            switch (metric.getType()) {
                case SUM:
                    termsBuilder.subAggregation(AggregationBuilders.sum(metric.getName()).field(metric.getField()));
                    break;
                case AVG:
                    termsBuilder.subAggregation(AggregationBuilders.avg(metric.getName()).field(metric.getField()));
                    break;
                case DISTINCT_COUNT:
                    termsBuilder.subAggregation(AggregationBuilders.cardinality(metric.getName()).field(metric.getField()));
                    break;
                case MAX:
                    termsBuilder.subAggregation(AggregationBuilders.max(metric.getName()).field(metric.getField()));
                    break;
                case MIN:
                    termsBuilder.subAggregation(AggregationBuilders.min(metric.getName()).field(metric.getField()));
                    break;
            }
        }
        return termsBuilder;
    }

}

