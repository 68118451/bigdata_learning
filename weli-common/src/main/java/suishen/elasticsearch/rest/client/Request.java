package suishen.elasticsearch.rest.client;

import com.alibaba.fastjson.JSONObject;
import com.flink.elasticsearch.core.query.*;
import com.flink.sink.es.elasticsearch.core.query.*;
import suishen.elasticsearch.core.query.*;
import suishen.elasticsearch.rest.client.meta.UpdateBulkMeta;
import suishen.elasticsearch.util.JsonUtils;
import com.google.common.base.Charsets;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.search.fetch.source.FetchSourceContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Author: Alvin Li
 * Date: 09/08/2017
 * Time: 18:10
 */
public class Request {
    private static final Logger LOG = LoggerFactory.getLogger(Request.class);

    final String method;
    final String endpoint;
    final Map<String, String> params;
    final HttpEntity entity;

    public static final char separator = '\n';

    public static final String EMPTY_UPDATE_DOC;

    static {
        JSONObject doc = new JSONObject();
        doc.put("doc", new JSONObject());
        EMPTY_UPDATE_DOC = doc.toJSONString();
    }

    public Request(String method, String endpoint, Map<String, String> params, HttpEntity entity) {
        this.method = method;
        this.endpoint = endpoint;
        this.params = params;
        this.entity = entity;
    }

    @Override
    public String toString() {
        return "Request{" +
                "method='" + method + '\'' +
                ", endpoint='" + endpoint + '\'' +
                ", params=" + params +
                ", entity=" + entity +
                '}';
    }

    private static byte[] toBytes(BytesReference reference) {
        final BytesRef bytesRef = reference.toBytesRef();
        if (bytesRef.offset == 0 && bytesRef.length == bytesRef.bytes.length) {
            return bytesRef.bytes;
        }
        return BytesRef.deepCopyOf(bytesRef).bytes;
    }

    static Request bulk(final BulkRequest bulkRequest, TimeValue timeout) throws IOException {
        if (bulkRequest.isEmpty()) {
            return null;
        }
        Params parameters = Params.builder();
        parameters.withTimeout(timeout);

        ByteArrayOutputStream content = new ByteArrayOutputStream();

        for (BulkOperation request : bulkRequest.getRequests()) {
            BulkOperationType opType = request.opType();
            try (XContentBuilder metadata = XContentBuilder.builder(XContentType.JSON.xContent())) {
                metadata.startObject();
                {
                    metadata.startObject(opType.getLowercase());
                    if (StringUtils.isNotBlank(request.getIndexName())) {
                        metadata.field("_index", request.getIndexName());
                    }
                    if (StringUtils.isNotBlank(request.getType())) {
                        metadata.field("_type", request.getType());
                    }
                    if (StringUtils.isNotBlank(request.getId())) {
                        metadata.field("_id", request.getId());
                    }
                    if (opType == BulkOperationType.UPDATE) {
                        metadata.field("_retry_on_conflict", 3);
                    }
                    metadata.endObject();
                }
                metadata.endObject();

                BytesRef metadataSource = metadata.bytes().toBytesRef();
                content.write(metadataSource.bytes, metadataSource.offset, metadataSource.length);
                content.write(separator);

            }

            BytesRef source = null;
            String objSource;

            if (opType == BulkOperationType.INDEX) {
                if (request.getObject() != null) {
                    objSource = JsonUtils.object2Json(request.getObject());
                } else if (StringUtils.isNotBlank(request.getSource())) {
                    objSource = request.getSource();
                } else {
                    objSource = JsonUtils.EMPTY_JSON_OBJECT_STR;
                }

                if (StringUtils.isNotBlank(objSource)) {
                    source = new BytesRef(objSource.getBytes(Charsets.UTF_8));
                }
            }

            if (opType == BulkOperationType.UPDATE) {
                if (request.getObject() != null) {
                    objSource = JsonUtils.object2Json(new UpdateBulkMeta(request.getObject(), request.isUpsert()));
                } else if (StringUtils.isNotBlank(request.getSource())) {
                    JSONObject doc = new JSONObject();
                    JSONObject docContent = JSONObject.parseObject(request.getSource());
                    doc.put("doc", docContent);
                    if (request.isUpsert()) {
                        doc.put("doc_as_upsert", true);
                    }
                    objSource = doc.toJSONString();
                } else {
                    objSource = EMPTY_UPDATE_DOC;
                }

                if (StringUtils.isNotBlank(objSource)) {
                    source = new BytesRef(objSource.getBytes(Charsets.UTF_8));
                }
            }

            if (source != null) {
                content.write(source.bytes, source.offset, source.length);
                content.write(separator);
            }

        }

        LOG.debug(content.toString(Charsets.UTF_8.name()));
        HttpEntity entity = new ByteArrayEntity(content.toByteArray(), 0, content.size(), ContentType.APPLICATION_JSON);
        return new Request(HttpPost.METHOD_NAME, "/_bulk", parameters.getParams(), entity);
    }

    static Request index(final IndexQuery indexQuery, TimeValue timeout) {
        String method = Strings.hasLength(indexQuery.getId()) ? HttpPut.METHOD_NAME : HttpPost.METHOD_NAME;
        String endpoint = endpoint(indexQuery.getIndexName(), indexQuery.getType(), indexQuery.getId());

        Params parameters = Params.builder();
        parameters.withTimeout(timeout);
        if (indexQuery.getVersion() != null) {
            parameters.withVersion(indexQuery.getVersion());
        }

        HttpEntity entity;

        if (indexQuery.getObject() != null) {
            entity = createJsonBodyEntity(indexQuery.getObject());
        } else if (StringUtils.isNotBlank(indexQuery.getSource())) {
            entity = new ByteArrayEntity(indexQuery.getSource().getBytes(Charsets.UTF_8), ContentType.APPLICATION_JSON);
        } else {
            entity = new ByteArrayEntity(JsonUtils.EMPTY_JSON_OBJECT_STR.getBytes(Charsets.UTF_8), ContentType.APPLICATION_JSON);
        }

        return new Request(method, endpoint, parameters.getParams(), entity);
    }


    static Request update(final UpdateQuery updateQuery, TimeValue timeout) {
        String endpoint = endpoint(updateQuery.getIndexName(), updateQuery.getType(), updateQuery.getId(), "_update");
        Params parameters = Params.builder();
        parameters.withTimeout(timeout);
        parameters.withDocAsUpsert(updateQuery.isUpsert());
        if (updateQuery.getVersion() != null) {
            parameters.withVersion(updateQuery.getVersion());
        }

        HttpEntity entity;
        if (updateQuery.getObject() != null) {
            entity = createJsonBodyEntity(new UpdateBulkMeta(updateQuery.getObject(), updateQuery.isUpsert()));
        } else if (StringUtils.isNotBlank(updateQuery.getSource())) {
            JSONObject doc = new JSONObject();
            JSONObject docContent = JSONObject.parseObject(updateQuery.getSource());
            doc.put("doc", docContent);
            if (updateQuery.isUpsert()) {
                doc.put("doc_as_upsert", true);
            }
            entity = new ByteArrayEntity(doc.toJSONString().getBytes(Charsets.UTF_8), ContentType.APPLICATION_JSON);
        } else {
            entity = new ByteArrayEntity(EMPTY_UPDATE_DOC.getBytes(Charsets.UTF_8), ContentType.APPLICATION_JSON);
        }

        return new Request(HttpPost.METHOD_NAME, endpoint, parameters.getParams(), entity);
    }

    static Request delete(final DeleteQuery deleteQuery, TimeValue timeout) {
        String endpoint = endpoint(deleteQuery.getIndexName(), deleteQuery.getType(), deleteQuery.getId());

        Params parameters = Params.builder();
        parameters.withTimeout(timeout);
        if (deleteQuery.getVersion() != null) {
            parameters.withVersion(deleteQuery.getVersion());
        }

        return new Request(HttpDelete.METHOD_NAME, endpoint, parameters.getParams(), null);
    }

    static String endpoint(String[] indices, String[] types, String endpoint) {
        return endpoint(StringUtils.join(indices, ','), StringUtils.join(types, ','), endpoint);
    }

    static String endpoint(String... parts) {
        StringBuilder sb = new StringBuilder("/");
        for (String part : parts) {
            if (StringUtils.isNotBlank(part)) {
                sb.append(part);
                sb.append("/");
            }
        }
        String result = sb.toString();

        return result.substring(0, result.length() - 1);
    }

    private static HttpEntity createJsonBodyEntity(Object item) {
        String entity = JsonUtils.object2Json(item);
        return new ByteArrayEntity(entity.getBytes(Charsets.UTF_8), ContentType.APPLICATION_JSON);
    }

    /**
     * Utility class to build request's parameters map and centralize all parameter names.
     */
    static class Params {
        private final Map<String, String> params = new HashMap<>();

        private Params() {
        }

        Params putParam(String key, String value) {
            if (Strings.hasLength(value)) {
                if (params.containsKey(key)) {
                    throw new IllegalArgumentException("Request parameter [" + key + "] is already registered");
                } else {
                    params.put(key, value);
                }
            }
            return this;
        }

        Params putParam(String key, TimeValue value) {
            if (value != null) {
                return putParam(key, value.toString());
            }
            return this;
        }

        Params withDocAsUpsert(boolean docAsUpsert) {
            if (docAsUpsert) {
                return putParam("doc_as_upsert", Boolean.TRUE.toString());
            }
            return this;
        }

        Params withFetchSourceContext(FetchSourceContext fetchSourceContext) {
            if (fetchSourceContext != null) {
                if (fetchSourceContext.fetchSource() == false) {
                    putParam("_source", Boolean.FALSE.toString());
                }
                if (fetchSourceContext.includes() != null && fetchSourceContext.includes().length > 0) {
                    putParam("_source_include", StringUtils.joinWith(",", fetchSourceContext.includes()));
                }
                if (fetchSourceContext.excludes() != null && fetchSourceContext.excludes().length > 0) {
                    putParam("_source_exclude", StringUtils.joinWith(",", fetchSourceContext.excludes()));
                }
            }
            return this;
        }

        Params withParent(String parent) {
            return putParam("parent", parent);
        }

        Params withPipeline(String pipeline) {
            return putParam("pipeline", pipeline);
        }

        Params withPreference(String preference) {
            return putParam("preference", preference);
        }

        Params withRealtime(boolean realtime) {
            if (realtime == false) {
                return putParam("realtime", Boolean.FALSE.toString());
            }
            return this;
        }

        Params withRetryOnConflict(int retryOnConflict) {
            if (retryOnConflict > 0) {
                return putParam("retry_on_conflict", String.valueOf(retryOnConflict));
            }
            return this;
        }

        Params withRouting(String routing) {
            return putParam("routing", routing);
        }

        Params withStoredFields(String[] storedFields) {
            if (storedFields != null && storedFields.length > 0) {
                return putParam("stored_fields", StringUtils.joinWith(",", storedFields));
            }
            return this;
        }

        Params withTimeout(TimeValue timeout) {
            return putParam("timeout", timeout);
        }

        Params withVersion(long version) {
            if (version != Versions.MATCH_ANY) {
                return putParam("version", Long.toString(version));
            }
            return this;
        }

        Params withVersionType(VersionType versionType) {
            if (versionType != VersionType.INTERNAL) {
                return putParam("version_type", versionType.name().toLowerCase(Locale.ROOT));
            }
            return this;
        }

        Params withIndicesOptions(IndicesOptions indicesOptions) {
            putParam("ignore_unavailable", Boolean.toString(indicesOptions.ignoreUnavailable()));
            putParam("allow_no_indices", Boolean.toString(indicesOptions.allowNoIndices()));
            String expandWildcards;
            if (indicesOptions.expandWildcardsOpen() == false && indicesOptions.expandWildcardsClosed() == false) {
                expandWildcards = "none";
            } else {
                StringBuilder sb = new StringBuilder();
                if (indicesOptions.expandWildcardsOpen()) {
                    sb.append("open");
                    sb.append(",");
                }
                if (indicesOptions.expandWildcardsClosed()) {
                    sb.append("closed");
                }
                expandWildcards = sb.toString();
            }
            putParam("expand_wildcards", expandWildcards);
            return this;
        }

        Map<String, String> getParams() {
            return Collections.unmodifiableMap(params);
        }

        static Params builder() {
            return new Params();
        }
    }

}
