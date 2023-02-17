package com.suishen.elasticsearch.meta.annotations.aggs.metrics;

import com.suishen.elasticsearch.meta.meta.query.MetricType;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Author: Alvin Li
 * Date: 12/06/2017
 * Time: 15:55
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface MetricAgg {
    MetricType type() default MetricType.SUM;
}
