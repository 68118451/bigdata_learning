package com.suishen.elasticsearch.meta.annotations.textQuery;


import com.suishen.elasticsearch.meta.meta.req.BoolType;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Author: Alvin Li
 * Date: 12/21/16
 * Time: 16:20
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface MatchQuery {
    /**
     * @return 对应ES中的field
     */
    String field();

    BoolType boolType() default BoolType.MUST;

    boolean dynamicField() default false;
}
