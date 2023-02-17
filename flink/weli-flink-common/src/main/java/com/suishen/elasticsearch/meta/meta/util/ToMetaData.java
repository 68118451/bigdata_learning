package com.suishen.elasticsearch.meta.meta.util;

import com.suishen.elasticsearch.meta.annotations.aggs.bucket.DateHistogramAgg;
import com.suishen.elasticsearch.meta.annotations.aggs.bucket.TermsAgg;
import com.suishen.elasticsearch.meta.annotations.nested.Nested;
import com.suishen.elasticsearch.meta.annotations.nested.NestedDynamicPath;
import com.suishen.elasticsearch.meta.annotations.termLevelQuery.DateRangeQuery;
import com.suishen.elasticsearch.meta.annotations.termLevelQuery.NumRangeQuery;
import com.suishen.elasticsearch.meta.annotations.termLevelQuery.TermOrTermsQuery;
import com.suishen.elasticsearch.meta.annotations.textQuery.MatchQuery;
import com.suishen.elasticsearch.meta.exception.DataTypeNotSupportException;
import com.suishen.elasticsearch.meta.meta.query.MetaDataCombination;
import com.suishen.elasticsearch.meta.meta.query.MetricType;
import com.suishen.elasticsearch.meta.meta.query.NumType;
import com.suishen.elasticsearch.meta.meta.req.BoolType;
import com.suishen.elasticsearch.meta.meta.req.ReqListMeta;
import com.suishen.elasticsearch.meta.meta.req.ReqObjectMeta;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.text.WordUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.*;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 将请求bean转换成指示生成ES Query的元数据bean的工具类
 * Author: Alvin Li
 * Date: 7/13/16
 * Time: 10:56
 */
public class ToMetaData {

    private static final Logger LOG = LoggerFactory.getLogger(ToMetaData.class);


    /**
     * 将请求bean转换成bool中包含nested查询的元数据bean
     *
     * @param bean
     * @return
     */
    public static MetaDataCombination toBoolWithNestedMeta(Object bean) {
        MetaDataCombination.MetaDataCombinationBuilder builder = MetaDataCombination.newBuilder();
        Field[] fields = bean.getClass().getDeclaredFields();
        Map<String, MetaDataCombination.MetaDataCombinationBuilder> nestedBoolBuilderMap = new HashMap<>();
        Map<String, BoolType> nestedBoolTypeMap = new HashMap<>();
        Map<String, String> nestedDynamicFieldPathMap = new HashMap<>();
        Map<String, String> dynamicFieldFieldMap = new HashMap<>();
        //提取动态的，field与path、field的映射
        for (Field f : fields) {
            if (f.isAnnotationPresent(NestedDynamicPath.class)) {
                Class<?> fieldClazz = f.getType();
                if (fieldClazz != ReqObjectMeta.class) {
                    throw new DataTypeNotSupportException("NestedDynamicPath field must be ReqObjectMeta<String>");
                }

                Class<?> paramClazz = getParamsClassType(f);
                if (paramClazz != String.class) {
                    throw new DataTypeNotSupportException("NestedDynamicPath value must be String");
                }

                Object value = getFieldValue(bean, f);
                if (value == null) {
                    continue;
                }

                ReqObjectMeta<String> lValue = (ReqObjectMeta<String>) value;
                String sValue = lValue.getValue();
                if (StringUtils.isBlank(sValue)) {
                    continue;
                }
                NestedDynamicPath anno = f.getAnnotation(NestedDynamicPath.class);
                for (String innerField : anno.fields()) {
                    nestedDynamicFieldPathMap.put(innerField, sValue);
                }
            }

            //TODO 暂无需求
//            if (f.isAnnotationPresent(DynamicField.class)) {
//                Class<?> fieldClazz = f.getType();
//                if (fieldClazz != ReqObjectMeta.class) {
//                    throw new DataTypeNotSupportException("DynamicField field must be ReqObjectMeta<String>");
//                }
//
//                Class<?> paramClazz = getParamsClassType(f);
//                if (paramClazz != String.class) {
//                    throw new DataTypeNotSupportException("DynamicField value must be String");
//                }
//
//                Object value = getFieldValue(bean, f);
//                if (value == null) {
//                    continue;
//                }
//
//                ReqObjectMeta<String> lValue = (ReqObjectMeta<String>) value;
//                String sValue = lValue.getValue();
//                if (StringUtils.isBlank(sValue)) {
//                    continue;
//                }
//                DynamicField anno = f.getAnnotation(DynamicField.class);
//                for (String innerField : anno.fields()) {
//                    dynamicFieldFieldMap.put(innerField, sValue);
//                }
//            }


        }
        //
        for (Field f : fields) {
            checkFieldType(f);
            Object value = getFieldValue(bean, f);
            if (f.isAnnotationPresent(Nested.class)) {
                buildNestedQuery(f, value, nestedBoolBuilderMap, nestedBoolTypeMap, nestedDynamicFieldPathMap, dynamicFieldFieldMap);
            } else if (f.isAnnotationPresent(NestedDynamicPath.class)) {
            } else {
                buildCommonQuery(builder, f, value, dynamicFieldFieldMap);
            }
        }
        for (Map.Entry<String, MetaDataCombination.MetaDataCombinationBuilder> entry : nestedBoolBuilderMap.entrySet()) {
            builder.addNestedQuery(entry.getKey(), nestedBoolTypeMap.get(entry.getKey()), entry.getValue().build());
        }
        return builder.build();
    }

    /**
     * 将请求bean转换成供bool查询的元数据bean
     *
     * @param bean 请求bean
     * @return
     */
    public static MetaDataCombination toBoolQueryMeta(Object bean) {
        MetaDataCombination.MetaDataCombinationBuilder builder = MetaDataCombination.newBuilder();
        Field[] fields = bean.getClass().getDeclaredFields();

        Map<String, String> dynamicFieldFieldMap = new HashMap<>();
        //TODO 暂无需求：获得所有NestedDynamicPath的内容，field与path的映射
//        for (Field f : fields) {
//            if (f.isAnnotationPresent(DynamicField.class)) {
//                Class<?> fieldClazz = f.getType();
//                if (fieldClazz != ReqObjectMeta.class) {
//                    throw new DataTypeNotSupportException("DynamicField field must be ReqObjectMeta<String>");
//                }
//
//                Class<?> paramClazz = getParamsClassType(f);
//                if (paramClazz != String.class) {
//                    throw new DataTypeNotSupportException("DynamicField value must be String");
//                }
//
//                Object value = getFieldValue(bean, f);
//                if (value == null) {
//                    continue;
//                }
//
//                ReqObjectMeta<String> lValue = (ReqObjectMeta<String>) value;
//                String sValue = lValue.getValue();
//                if (StringUtils.isBlank(sValue)) {
//                    continue;
//                }
//                DynamicField anno = f.getAnnotation(DynamicField.class);
//                for (String innerField : anno.fields()) {
//                    dynamicFieldFieldMap.put(innerField, sValue);
//                }
//            }
//        }

        for (Field f : fields) {
            checkFieldType(f);
            Object value = getFieldValue(bean, f);
            buildCommonQuery(builder, f, value, dynamicFieldFieldMap);
        }

        return builder.build();
    }

    /**
     * 构建通用的查询元数据的集合,包含term/terms filter，date range,num range Filter
     *
     * @param f
     * @param value
     * @param builder
     */
    private static void buildCommonQuery(MetaDataCombination.MetaDataCombinationBuilder builder, Field f, Object value, final Map<String, String> dynamicFieldFieldMap) {
        if (value == null) {
            return;
        }

        if (f.isAnnotationPresent(DateRangeQuery.class)) {
            buildDateRangeQuery(builder, f, value, dynamicFieldFieldMap);
        } else if (f.isAnnotationPresent(NumRangeQuery.class)) {
            buildNumRangeQuery(builder, f, value, dynamicFieldFieldMap);
        } else if (f.isAnnotationPresent(MatchQuery.class)) {
            buildMatchQuery(builder, f, value, dynamicFieldFieldMap);
        } else if (f.isAnnotationPresent(TermOrTermsQuery.class)) {
            buildTermsQuery(builder, f, value, dynamicFieldFieldMap);
        } else if (f.isAnnotationPresent(DateHistogramAgg.class)) {
            buildDateHistogramAgg(builder, f, value);
        } else if (f.isAnnotationPresent(TermsAgg.class)) {
            buildTermsAgg(builder, f, value);
        } else {
            //暂时不做操作
        }
    }


    /**
     * 构建nested query
     *
     * @param f
     * @param value
     */
    private static void buildNestedQuery(Field f, Object value,
                                         Map<String, MetaDataCombination.MetaDataCombinationBuilder> nestedBoolBuilderMap,
                                         Map<String, BoolType> nestedBoolTypeMap,
                                         Map<String, String> nestedDynamicFieldPathMap,
                                         final Map<String, String> dynamicFieldFieldMap) {
        Nested nestedAnno = f.getAnnotation(Nested.class);
        String path;
        //获得path
        if (nestedAnno.dynamicPath()) {
            String fieldName = f.getName();
            path = nestedDynamicFieldPathMap.get(fieldName);
        } else {
            path = nestedAnno.path();
        }
        if (StringUtils.isBlank(path)) {
            return;
        }
        //构造查询
        nestedBoolTypeMap.put(path, nestedAnno.boolType());
        MetaDataCombination.MetaDataCombinationBuilder nestedBuilder = nestedBoolBuilderMap.get(path);
        if (nestedBuilder == null) {
            nestedBuilder = MetaDataCombination.newBuilder(path);
            nestedBoolBuilderMap.put(path, nestedBuilder);
        }
        buildCommonQuery(nestedBuilder, f, value, dynamicFieldFieldMap);

    }


    private static void buildTermsAgg(MetaDataCombination.MetaDataCombinationBuilder builder, Field f, Object value) {
        Class<?> fieldClazz = f.getType();
        if (fieldClazz != ReqObjectMeta.class) {
            throw new DataTypeNotSupportException("num range value must be ReqObjectMeta<String>");
        }
        Class<?> paramClazz = getParamsClassType(f);
        if (paramClazz == Boolean.class) {
            ReqObjectMeta<Boolean> lValue = (ReqObjectMeta<Boolean>) value;
            TermsAgg anno = f.getAnnotation(TermsAgg.class);
            if (!lValue.getValue()) {
                return;
            }
            builder.setTermsAggMeta(anno.name(), anno.field());
            builder.setTermsAggDoAgg(lValue.getValue());

            String[] metricName = anno.metricNames();
            MetricType[] metricType = anno.metricType();
            String[] metricField = anno.metricField();
            for (int i = 0; i < metricName.length; i++) {
                builder.addTermsMetric(metricName[i], metricType[i], metricField[i]);
            }
        } else {
            throw new DataTypeNotSupportException("dateHistogram value must be boolean");
        }
    }

    private static void buildDateHistogramAgg(MetaDataCombination.MetaDataCombinationBuilder builder, Field f, Object value) {
        Class<?> fieldClazz = f.getType();
        if (fieldClazz != ReqObjectMeta.class) {
            throw new DataTypeNotSupportException("num range value must be ReqObjectMeta<String>");
        }

        Class<?> paramClazz = getParamsClassType(f);
        if (paramClazz == Boolean.class) {
            ReqObjectMeta<Boolean> lValue = (ReqObjectMeta<Boolean>) value;
            DateHistogramAgg anno = f.getAnnotation(DateHistogramAgg.class);
            if (!lValue.getValue()) {
                return;
            }
            builder.setDateHistogramDoAgg(lValue.getValue());
            builder.addDateHistogram(anno.name(), anno.field(), anno.interval(), anno.intervalLength(), anno.minDoc(), anno.format());
            builder.setDateHistogramExtendBound(anno.extendBoundMin(), anno.extendBoundMax());
            String[] metricName = anno.metricNames();
            MetricType[] metricType = anno.metricType();
            String[] metricField = anno.metricField();
            for (int i = 0; i < metricName.length; i++) {
                builder.addDateHistogramMetric(metricName[i], metricType[i], metricField[i]);
            }
        } else {
            throw new DataTypeNotSupportException("dateHistogram value must be boolean");
        }

    }


    /**
     * 构建日期范围的过滤
     *
     * @param builder
     * @param f
     * @param value
     */
    private static void buildDateRangeQuery(MetaDataCombination.MetaDataCombinationBuilder builder, Field f, Object value, final Map<String, String> dynamicFieldFieldMap) {

        Class<?> fieldClazz = f.getType();
        if (fieldClazz != ReqObjectMeta.class) {
            throw new DataTypeNotSupportException("date range value must be ReqObjectMeta<>");
        }
        ReqObjectMeta<Object> lValue = (ReqObjectMeta<Object>) value;
        DateRangeQuery anno = f.getAnnotation(DateRangeQuery.class);
        builder.addDateRangeQuery(anno.field(), anno.rangeType(), anno.boolType(), String.valueOf(lValue.getValue()), anno.format());
    }

    /**
     * 构建数值范围的过滤
     *
     * @param builder
     * @param f
     * @param value
     */
    private static void buildNumRangeQuery(MetaDataCombination.MetaDataCombinationBuilder builder, Field f, Object value, final Map<String, String> dynamicFieldFieldMap) {
        Class<?> fieldClazz = f.getType();

        if (fieldClazz != ReqObjectMeta.class) {
            throw new DataTypeNotSupportException("num range value must be ReqObjectMeta<String>");
        }

        BigDecimal numValue = null;
        Class<?> paramClazz = getParamsClassType(f);
        if (paramClazz == int.class || paramClazz == Integer.class ||
                paramClazz == long.class || paramClazz == Long.class ||
                paramClazz == float.class || paramClazz == Float.class ||
                paramClazz == double.class || paramClazz == Double.class ||
                paramClazz == BigDecimal.class) {
            ReqObjectMeta<Object> lValue = (ReqObjectMeta<Object>) value;
            numValue = new BigDecimal(lValue.getValue().toString());
        } else {
            throw new DataTypeNotSupportException("num range value must be number");
        }
        NumRangeQuery anno = f.getAnnotation(NumRangeQuery.class);
        builder.addNumberRangeQuery(anno.field(), anno.rangeType(), anno.boolType(), numValue);

    }

    /**
     * 构建term是查询
     *
     * @param builder
     * @param f
     * @param value
     */
    private static void buildMatchQuery(MetaDataCombination.MetaDataCombinationBuilder builder, Field f, Object value, final Map<String, String> dynamicFieldFieldMap) {
        Class<?> fieldClazz = f.getType();
        MatchQuery anno = f.getAnnotation(MatchQuery.class);
        if (fieldClazz == ReqObjectMeta.class) {
            Class<?> paramClazz = getParamsClassType(f);
            if (paramClazz == String.class) {
                ReqObjectMeta<String> lValue = (ReqObjectMeta<String>) value;
                String sValue = lValue.getValue();
                if (StringUtils.isBlank(sValue)) {
                    return;
                }
                builder.addMatchQuery(anno.field(), anno.boolType(), sValue);
            } else {
                throw new DataTypeNotSupportException("terms filter value must be string ");
            }
        } else {
            throw new DataTypeNotSupportException("field data must be ReqObjectMeta");
        }
    }

    /**
     * 构建term是查询
     *
     * @param builder
     * @param f
     * @param value
     */
    private static void buildTermsQuery(MetaDataCombination.MetaDataCombinationBuilder builder, Field f, Object value, final Map<String, String> dynamicFieldFieldMap) {
        Class<?> fieldClazz = f.getType();
        TermOrTermsQuery anno = f.getAnnotation(TermOrTermsQuery.class);
        if (fieldClazz == ReqListMeta.class) {
            Class<?> paramClazz = getParamsClassType(f);
            if (paramClazz == String.class) {
                ReqListMeta<String> rValue;
                String include_value = anno.include_value_str().trim();

                rValue = (ReqListMeta<String>) value;
                if (StringUtils.isNotBlank(include_value)) {
                    rValue.add(include_value);
                }


                builder.addStrTermsQuery(anno.field(), anno.boolType(), rValue.getValue().toArray(new String[rValue.size()]));

            } else if (paramClazz == int.class || paramClazz == Integer.class ||
                    paramClazz == long.class || paramClazz == Long.class ||
                    paramClazz == float.class || paramClazz == Float.class ||
                    paramClazz == double.class || paramClazz == Double.class ||
                    paramClazz == BigDecimal.class) {
                ReqListMeta<Object> oValue;
                Integer include_value = anno.include_value_num();

                oValue = (ReqListMeta<Object>) value;
                if (Integer.MIN_VALUE != include_value) {
                    oValue.add(include_value);
                }


                List<BigDecimal> bdValue = new ArrayList<>();
                for (Object o : oValue.getValue()) {
                    bdValue.add(new BigDecimal(o.toString()));
                }
                if (paramClazz == int.class || paramClazz == Integer.class || paramClazz == long.class || paramClazz == Long.class) {
                    builder.addNumTermsQuery(anno.field(), NumType.LONG, anno.boolType(), bdValue.toArray(new BigDecimal[bdValue.size()]));
                } else {
                    builder.addNumTermsQuery(anno.field(), NumType.DOUBLE, anno.boolType(), bdValue.toArray(new BigDecimal[bdValue.size()]));
                }
            } else {
                throw new DataTypeNotSupportException("terms filter value must be string or number");
            }
        } else if (fieldClazz == ReqObjectMeta.class) {
            Class<?> paramClazz = getParamsClassType(f);
            if (paramClazz == String.class) {
                ReqObjectMeta<String> lValue;
                if (value == null) {
                    return;
                } else {
                    lValue = (ReqObjectMeta<String>) value;
                }

                String sValue = lValue.getValue();
                if (StringUtils.isBlank(sValue)) {
                    return;
                }
                builder.addStrTermsQuery(anno.field(), anno.boolType(), sValue);
            } else if (paramClazz == int.class || paramClazz == Integer.class ||
                    paramClazz == long.class || paramClazz == Long.class ||
                    paramClazz == float.class || paramClazz == Float.class ||
                    paramClazz == double.class || paramClazz == Double.class ||
                    paramClazz == BigDecimal.class) {
                ReqObjectMeta<Object> lValue;
                if (value == null) {
                    return;
                } else {
                    lValue = (ReqObjectMeta<Object>) value;
                }

                BigDecimal bdValue = new BigDecimal(lValue.getValue().toString());
                if (paramClazz == int.class || paramClazz == Integer.class || paramClazz == long.class || paramClazz == Long.class) {
                    builder.addNumTermsQuery(anno.field(), NumType.LONG, anno.boolType(), bdValue);
                } else {
                    builder.addNumTermsQuery(anno.field(), NumType.DOUBLE, anno.boolType(), bdValue);
                }
            } else {
                throw new DataTypeNotSupportException("terms filter value must be string or number");
            }
        } else {
            throw new DataTypeNotSupportException("field data rangeType doesn't supported");
        }
    }

    /**
     * 通过get方法获得该field的值
     *
     * @param target
     * @param f
     * @return
     */
    private static Object getFieldValue(Object target, Field f) {
        try {
            String fieldName = f.getName();
            Class beanClazz = target.getClass();
            Method m = null;
            m = beanClazz.getMethod("get" + WordUtils.capitalize(fieldName));
            return m.invoke(target);
        } catch (NoSuchMethodException e) {
            LOG.error("bean don't have method" + "get" + WordUtils.capitalize(f.getName()), e);
            return null;
        } catch (InvocationTargetException e) {
            LOG.error("method" + "get" + WordUtils.capitalize(f.getName()) + "invoke error", e);
            return null;
        } catch (IllegalAccessException e) {
            LOG.error("bean don't have pemission of method" + "get" + WordUtils.capitalize(f.getName()), e);
            return null;
        }

    }

    /**
     * 获取泛型class类型
     *
     * @param f
     * @return
     */
    private static Class getParamsClassType(Field f) {
        Type gt = f.getGenericType();
        if (gt instanceof ParameterizedType) {
            //获得元素的类型
            ParameterizedType pt = (ParameterizedType) gt;
            return (Class<?>) pt.getActualTypeArguments()[0];
        } else {
            return Object.class;
        }
    }

    /**
     * 查询bean中的字段类型是否符合规范
     *
     * @param f
     */
    private static void checkFieldType(Field f) {
        Class<?> fieldClazz = f.getType();
        if (fieldClazz != ReqObjectMeta.class && fieldClazz != ReqListMeta.class) {
            throw new DataTypeNotSupportException("request field data rangeType should be ReqObjectMeta or ReqListMeta");
        }
        Type gt = f.getGenericType();
        if (gt instanceof ParameterizedType) {
            //获得元素的类型
            ParameterizedType pt = (ParameterizedType) gt;
            Class<?> paramClazz = (Class<?>) pt.getActualTypeArguments()[0];
            if (paramClazz != String.class && paramClazz != int.class && paramClazz != Integer.class &&
                    paramClazz != long.class && paramClazz != Long.class &&
                    paramClazz != float.class && paramClazz != Float.class &&
                    paramClazz != double.class && paramClazz != Double.class &&
                    paramClazz != BigDecimal.class && paramClazz != Boolean.class) {
                throw new DataTypeNotSupportException("ReqObjectMeta or ReqListMeta generic rangeType should be String or one of Number rangeType");
            }
        }
    }


}
