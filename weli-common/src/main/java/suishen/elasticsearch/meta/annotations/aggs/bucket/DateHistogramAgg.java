package suishen.elasticsearch.meta.annotations.aggs.bucket;


import suishen.elasticsearch.meta.meta.query.MetricType;
import suishen.elasticsearch.meta.meta.query.DateHistogramIntervalType;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Author: Alvin Li
 * Date: 12/06/2017
 * Time: 15:58
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface DateHistogramAgg {

    /**
     * @return 聚合操作的名称
     */
    String name();

    /**
     * @return 表示ES中进行日期聚合的字段名
     */
    String field();

    /**
     * @return DateHistogramAgg Agg 的统计时间间隔单位
     */
    DateHistogramIntervalType interval() default DateHistogramIntervalType.DAY;

    /**
     * @return DateHistogramAgg Agg 的统计时间间隔单位
     */
    int intervalLength() default 1;

    /**
     * @return DateHistogramAgg Agg 后返回统计的最小文档数
     */
    int minDoc() default 0;

    /**
     * @return 日期格式
     */
    String format() default "yyyyMMdd||yyyy-MM-dd";
    //对其它字段的统计

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

    /**
     * 时间的最小值
     */
    String extendBoundMin() default "";

    /**
     * 时间的最大值
     */
    String extendBoundMax() default "";

}
