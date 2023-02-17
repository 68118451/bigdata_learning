package com.suishen.elasticsearch.meta.annotations.nested;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 *
 * Author: Alvin Li
 * Date: 09/08/2017
 * Time: 14:23
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface NestedDynamicPath {
    //指定该注解的nested path指定的field
    String[] fields();

}
