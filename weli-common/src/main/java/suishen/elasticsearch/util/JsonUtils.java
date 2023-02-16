package suishen.elasticsearch.util;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * 使用Jackson库进行json有object转换的工具类
 * Author: Alvin Li
 * Date: 3/29/17
 * Time: 14:28
 */
public class JsonUtils {

    private static final Logger LOG = LoggerFactory.getLogger(JsonUtils.class);

    private static ObjectMapper mapper = new ObjectMapper();

    //生成的json是 []的通用实体
    public static final List EMPTY_JSON_LIST = new ArrayList<>();

    //生成的json是 {}的通用实体
    public static final Object EMPTY_JSON_OBJECT = new HashMap<>();

    public static final String EMPTY_JSON_LIST_STR = "[]";

    public static final String EMPTY_JSON_OBJECT_STR = "{}";

    static {
        //TODO 此处可以对mapper进行一些参数设置
    }

    /**
     * 将对象转换为json字符串
     *
     * @param obj
     * @return 正常返回json字符串，否则返回""
     */
    public static String object2Json(Object obj) {
        if (obj == null) {
            return "";
        }
        try {
            ObjectWriter ow = mapper.writer();
            return ow.writeValueAsString(obj);
        } catch (Exception e) {
            LOG.error("json convert exception", e);
            return "";
        }
    }

    /**
     * 将json字符串转换为对象
     *
     * @param json
     * @param clazz 对象的class
     * @return 正常返回转换后的对象，否则返回null
     */
    public static <T> T json2Object(String json, Class<T> clazz) {
        if (json == null || "".equals(json)) {
            return null;
        }
        try {
            return mapper.readValue(json, clazz);
        } catch (JsonGenerationException e) {
            LOG.error("json convert exception", e);
            return null;
        } catch (JsonMappingException e) {
            LOG.error("json convert exception", e);
            return null;
        } catch (IOException e) {
            LOG.error("json convert exception", e);
            return null;
        }
    }

    /**
     * 将json字符串转换为对象
     *
     * @param json
     * @param typeReference
     * @return 正常返回转换后的对象，否则返回null
     */
    public static Object json2Object(String json, TypeReference typeReference) {
        if (json == null || "".equals(json)) {
            return null;
        }
        try {
            return mapper.readValue(json, typeReference);
        } catch (JsonGenerationException e) {
            LOG.error("json convert exception", e);
            return null;
        } catch (JsonMappingException e) {
            LOG.error("json convert exception", e);
            return null;
        } catch (IOException e) {
            LOG.error("json convert exception", e);
            return null;
        }
    }

    /**
     * 将json字符串转换为对象
     *
     * @param json
     * @param javaType
     * @return 正常返回转换后的对象，否则返回null
     */
    public static Object json2Object(String json, JavaType javaType) {
        if (json == null || "".equals(json)) {
            return null;
        }
        try {
            return mapper.readValue(json, javaType);
        } catch (JsonGenerationException e) {
            LOG.error("json convert exception", e);
            return null;
        } catch (JsonMappingException e) {
            LOG.error("json convert exception", e);
            return null;
        } catch (IOException e) {
            LOG.error("json convert exception", e);
            return null;
        }
    }

    /**
     * 将json字符串转换为对象
     *
     * @param json
     * @param itemClass list中的对象类型
     * @param <T>
     * @return 正常返回转换后的list对象，否则返回null
     */
    public static <T> List<T> json2List(String json, Class<T> itemClass) {
        return (List<T>) json2Object(json, mapper.getTypeFactory().constructCollectionType(List.class, itemClass));
    }

    /**
     * 将json字符串转换为List<String>
     *
     * @param json
     * @return
     */
    public static List<String> json2StringList(String json) {
        return json2List(json, String.class);
    }

    /**
     * 将json字符串转换为List<Integer>
     *
     * @param json
     * @return
     */
    public static List<Integer> json2IntegerList(String json) {
        return json2List(json, Integer.class);
    }

}
