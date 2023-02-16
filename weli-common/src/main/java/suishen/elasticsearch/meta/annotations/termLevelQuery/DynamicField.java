package suishen.elasticsearch.meta.annotations.termLevelQuery;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 动态指定查询bean中的field对应es中的字段
 * Author: Alvin Li
 * Date: 09/08/2017
 * Time: 16:04
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface DynamicField {
    String[] fields();
}
