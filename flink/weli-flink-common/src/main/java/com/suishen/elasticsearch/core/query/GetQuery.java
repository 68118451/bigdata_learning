package com.suishen.elasticsearch.core.query;

import com.suishen.elasticsearch.meta.meta.req.JsonSerializerType;
import org.apache.commons.collections4.CollectionUtils;


import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Author: Bryant Hang
 * Date: 16/6/15
 * Time: 11:01
 */
public class GetQuery {
    private String id;
    private String indexName;
    private String indexType;
    private JsonSerializerType jsonSerializerType;
    private List<String> sourceIncludes;
    private List<String> sourceExcludes;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getIndexName() {
        return indexName;
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    public String getIndexType() {
        return indexType;
    }

    public void setIndexType(String indexType) {
        this.indexType = indexType;
    }

    public JsonSerializerType getJsonSerializerType() {
        return jsonSerializerType;
    }

    public void setJsonSerializerType(JsonSerializerType jsonSerializerType) {
        this.jsonSerializerType = jsonSerializerType;
    }

    public List<String> getSourceIncludes() {
        return sourceIncludes;
    }

    public void setSourceIncludes(List<String> sourceIncludes) {
        this.sourceIncludes = sourceIncludes;
    }

    public List<String> getSourceExcludes() {
        return sourceExcludes;
    }

    public void setSourceExcludes(List<String> sourceExcludes) {
        this.sourceExcludes = sourceExcludes;
    }

    public static Builder newBuilder(){
        return new Builder();
    }

    public static class Builder {

        private String id;
        private String indexName;
        private String indexType;
        private JsonSerializerType jsonSerializerType;
        private List<String> sourceIncludes;
        private List<String> sourceExcludes;

        public Builder() {
            this.jsonSerializerType = JsonSerializerType.FASTJSON;
            this.sourceIncludes = new ArrayList<>();
            this.sourceExcludes = new ArrayList<>();
        }

        public Builder withId(String id) {
            this.id = id;
            return this;
        }

        public Builder withIndexName(String indexName) {
            this.indexName = indexName;
            return this;
        }

        public Builder withIndexType(String type) {
            this.indexType = type;
            return this;
        }

        public Builder withSerializerType(JsonSerializerType jsonSerializerType){
            this.jsonSerializerType = jsonSerializerType;
            return this;
        }

        public Builder addSourceExclude(String... excludes){
            Collections.addAll(this.sourceExcludes,excludes);
            return this;
        }

        public Builder addSourceInclude(String... includes){
            Collections.addAll(this.sourceIncludes,includes);
            return this;
        }

        public Builder addSourceExclude(List<String> excludes){
            if(CollectionUtils.isNotEmpty(excludes)){
                this.sourceExcludes.addAll(excludes);
            }
            return this;
        }

        public Builder addSourceInclude(List<String> includes){
            if(CollectionUtils.isNotEmpty(includes)) {
                this.sourceIncludes.addAll(includes);
            }
            return this;
        }

        public GetQuery build() {
            GetQuery query = new GetQuery();
            query.setId(this.id);
            query.setIndexName(this.indexName);
            query.setIndexType(this.indexType);
            query.setJsonSerializerType(this.jsonSerializerType);
            query.setSourceIncludes(this.sourceIncludes);
            query.setSourceExcludes(this.sourceExcludes);
            return query;
        }
    }
}
