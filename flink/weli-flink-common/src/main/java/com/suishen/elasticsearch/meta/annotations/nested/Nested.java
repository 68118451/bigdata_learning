package com.suishen.elasticsearch.meta.annotations.nested;



import com.suishen.elasticsearch.meta.meta.req.BoolType;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * nested查询的注释，和
 * Author: Alvin Li
 * Date: 10/04/2017
 * Time: 11:42
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Nested {
    String path();

    BoolType boolType() default BoolType.MUST;

    //标记该nested的path是否是动态的
    boolean dynamicPath() default false;
}
