package com.suishen.elasticsearch.meta.annotations.termLevelQuery;


import com.suishen.elasticsearch.meta.meta.req.BoolType;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 表示该字段会被当做ES 中term/terms query的内容
 * <p>
 * Author: Alvin Li
 * Date: 7/12/16
 * Time: 11:38
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface TermOrTermsQuery {

    /**
     * @return 对应ES中的field
     */
    String field();

    BoolType boolType() default BoolType.MUST;

    String include_value_str() default "";

    int include_value_num() default Integer.MIN_VALUE;

    boolean dynamicField() default false;
}
