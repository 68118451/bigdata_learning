package com.suishen.elasticsearch.core.query;

import org.apache.commons.lang3.StringUtils;

/**
 * Author: Alvin Li
 * Date: 12/13/16
 * Time: 17:09
 */
public class DeleteQuery implements BulkOperation{
    private String id;
    private String type;
    private Long version;
    private String indexName;
    private String parentId;


    public String getId() {
        return id;
    }

    @Override
    public Object getObject() {
        return null;
    }

    public void setId(String id) {
        this.id = id;
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
        return BulkOperationType.DELETE;
    }

    @Override
    public String getSource() {
        return null;
    }

    public void setType(String type) {
        this.type = type;
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

    public String getParentId() {
        return parentId;
    }

    public void setParentId(String parentId) {
        this.parentId = parentId;
    }

    public static Builder newBuilder(){
        return new Builder();
    }

    public static class Builder {
        private String id;
        private String type;
        private Long version;
        private String indexName;
        private String parentId;

        public Builder setId(String id) {
            this.id = id;
            return this;
        }

        public Builder setType(String type) {
            this.type = type;
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

        public Builder setParentId(String parentId) {
            this.parentId = parentId;
            return this;
        }

        public DeleteQuery build() {
            if(StringUtils.isBlank(id)
                    || StringUtils.isBlank(indexName)
                    || StringUtils.isBlank(type)){
                throw new IllegalArgumentException("id,index,type can't be empty");
            }
            DeleteQuery dq = new DeleteQuery();
            dq.setId(id);
            dq.setType(type);
            dq.setVersion(version);
            dq.setIndexName(indexName);
            dq.setParentId(parentId);
            return dq;
        }
    }


}
