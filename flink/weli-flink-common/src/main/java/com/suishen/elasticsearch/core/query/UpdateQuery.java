package com.suishen.elasticsearch.core.query;

import org.apache.commons.lang3.StringUtils;

/**
 * Author: Alvin Li
 * Date: 12/14/16
 * Time: 15:57
 */
public class UpdateQuery implements BulkOperation {
    private String id;
    private Object object;
    private Long version;
    private String indexName;
    private String type;
    private String source;
    private String parentId;
    private boolean isUpsert = true;

    @Override
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    @Override
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

    @Override
    public String getIndexName() {
        return indexName;
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    @Override
    public String getType() {
        return type;
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

    @Override
    public boolean isUpsert() {
        return isUpsert;
    }

    @Override
    public BulkOperationType opType() {
        return BulkOperationType.UPDATE;
    }

    public void setUpsert(boolean upsert) {
        isUpsert = upsert;
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
        private boolean isUpsert = true;

        public Builder setId(String id) {
            this.id = id;
            return this;
        }

        public Builder setObject(Object object) {
            this.object = object;
            return this;
        }

        public Builder setVersion(Long version) {
            this.version = version;
            return this;
        }

        public Builder setIndexName(String indexName) {
            this.indexName = indexName;
            return this;
        }

        public Builder setType(String type) {
            this.type = type;
            return this;
        }

        public Builder setSource(String source) {
            this.source = source;
            return this;
        }

        public Builder setParentId(String parentId) {
            this.parentId = parentId;
            return this;
        }

        public Builder setIsUpsert(boolean isUpsert) {
            this.isUpsert = isUpsert;
            return this;
        }

        public UpdateQuery build() {
            if (StringUtils.isBlank(id)
                    || StringUtils.isBlank(indexName)
                    || StringUtils.isBlank(type)) {
                throw new IllegalArgumentException("id,index,type can't be empty");
            }
            if (object == null && StringUtils.isBlank(source)) {
                throw new IllegalArgumentException("object and source cann't both be empty");
            }
            UpdateQuery updateQuery = new UpdateQuery();
            updateQuery.setId(id);
            updateQuery.setObject(object);
            updateQuery.setVersion(version);
            updateQuery.setIndexName(indexName);
            updateQuery.setType(type);
            updateQuery.setSource(source);
            updateQuery.setParentId(parentId);
            updateQuery.setUpsert(isUpsert);
            return updateQuery;
        }
    }
}
