package com.suishen.elasticsearch.meta.annotations.termLevelQuery;




import com.suishen.elasticsearch.meta.meta.query.RangeType;
import com.suishen.elasticsearch.meta.meta.req.BoolType;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 表示该字段会被当做ES 中Date Range filter的内容
 * <p>
 * Author: Alvin Li
 * Date: 7/12/16
 * Time: 11:42
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface DateRangeQuery {

    /**
     * @return 对应ES中的field
     */
    String field();

    /**
     * @return 对比的类型
     */
    RangeType rangeType();

    /**
     * @return 日期内容的格式
     */
    String format() default "yyyy-MM-dd HH:mm:ss||epoch_millis";

    BoolType boolType() default BoolType.FILTER;

    boolean dynamicField() default false;
}
