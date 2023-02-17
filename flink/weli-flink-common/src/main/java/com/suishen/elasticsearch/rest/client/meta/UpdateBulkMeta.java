package com.suishen.elasticsearch.rest.client.meta;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Author: Alvin Li
 * Date: 11/08/2017
 * Time: 11:08
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class UpdateBulkMeta {
    @JsonProperty("doc")
    private Object doc;

    @JsonProperty("doc_as_upsert")
    private Boolean docAsUpsert;

    public UpdateBulkMeta() {
    }

    public UpdateBulkMeta(Object doc, Boolean docAsUpsert) {
        this.doc = doc;
        this.docAsUpsert = docAsUpsert;
    }

    public Object getDoc() {
        return doc;
    }

    public void setDoc(Object doc) {
        this.doc = doc;
    }

    public Boolean getDocAsUpsert() {
        return docAsUpsert;
    }

    public void setDocAsUpsert(Boolean docAsUpsert) {
        this.docAsUpsert = docAsUpsert;
    }
}
