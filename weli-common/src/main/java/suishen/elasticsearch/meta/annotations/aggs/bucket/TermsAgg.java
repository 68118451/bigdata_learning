package suishen.elasticsearch.meta.annotations.aggs.bucket;


import suishen.elasticsearch.meta.meta.query.MetricType;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Author: Alvin Li
 * Date: 26/06/2017
 * Time: 12:01
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface TermsAgg {

    /**
     * @return 聚合操作的名称
     */
    String name();

    /**
     * @return 表示ES中进行日期聚合的字段名
     */
    String field();

    /**
     * 在DateHistogram Agg下附带进行的metric agg运算的名
     */
    String[] metricNames() default {};

    /**
     * 在DateHistogram Agg下附带进行的metric agg运算的ES字段
     */
    String[] metricField() default {};

    /**
     * 在DateHistogram Agg下附带进行metric agg运算的类型
     */
    MetricType[] metricType() default {};
}
