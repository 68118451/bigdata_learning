package suishen.elasticsearch.annotations;


import suishen.elasticsearch.meta.meta.req.JsonSerializerType;

import java.lang.annotation.*;

/**
 * Author: Bryant Hang
 * Date: 16/6/14
 * Time: 19:25
 */
@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface Document {

    String indexName() default "";

    String type() default "";

    JsonSerializerType jsonSerialiizer() default JsonSerializerType.FASTJSON;
}
