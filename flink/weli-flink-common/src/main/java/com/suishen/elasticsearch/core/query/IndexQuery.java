package com.suishen.elasticsearch.core.query;

import org.apache.commons.lang3.StringUtils;

/**
 * Author: Bryant Hang
 * Date: 16/6/14
 * Time: 18:56
 */
public class IndexQuery implements BulkOperation {

    private String id;
    private Object object;
    private Long version;
    private String indexName;
    private String type;
    private String source;
    private String parentId;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Object getObject() {
        return object;
    }

    public void setObject(Object object) {
        this.object = object;
    }

    public Long getVersion() {
        return version;
    }

    public void setVersion(Long version) {
        this.version = version;
    }

    public String getIndexName() {
        return indexName;
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    public String getType() {
        return type;
    }

    @Override
    public boolean isUpsert() {
        return false;
    }

    @Override
    public BulkOperationType opType() {
        return BulkOperationType.INDEX;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getParentId() {
        return parentId;
    }

    public void setParentId(String parentId) {
        this.parentId = parentId;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {

        private String id;
        private Object object;
        private Long version;
        private String indexName;
        private String type;
        private String source;
        private String parentId;

        public Builder withId(String id) {
            this.id = id;
            return this;
        }

        public Builder withObject(Object object) {
            this.object = object;
            return this;
        }

        public Builder withVersion(Long version) {
            this.version = version;
            return this;
        }

        public Builder withIndexName(String indexName) {
            this.indexName = indexName;
            return this;
        }

        public Builder withType(String type) {
            this.type = type;
            return this;
        }

        public Builder withSource(String source) {
            this.source = source;
            return this;
        }

        public Builder withParentId(String parentId) {
            this.parentId = parentId;
            return this;
        }

        public IndexQuery build() {
            if (StringUtils.isBlank(id)
                    || StringUtils.isBlank(indexName)
                    || StringUtils.isBlank(type)) {
                throw new IllegalArgumentException("id,index,type can't be empty");
            }
            if (object == null && StringUtils.isBlank(source)) {
                throw new IllegalArgumentException("object and source cann't both be empty");
            }
            IndexQuery indexQuery = new IndexQuery();
            indexQuery.setId(id);
            indexQuery.setIndexName(indexName);
            indexQuery.setType(type);
            indexQuery.setObject(object);
            indexQuery.setParentId(parentId);
            indexQuery.setSource(source);
            indexQuery.setVersion(version);
            return indexQuery;
        }
    }
}
