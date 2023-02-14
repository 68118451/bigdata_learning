package com.flink.cdc.bean;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.annotation.JSONField;
import lombok.Builder;
import lombok.Getter;

/**
 * description: MongoCdcCotent <br>
 * date: 2022-11-10 17:47 <br>
 * author: YQ <br>
 * version: 1.0 <br>
 */
@Builder
@Getter
public class MongoCdcCotent {

    @JSONField(name = "document_key")
    String documentKey;
    @JSONField(name = "operation_type")
    String operationType;
    @JSONField(name = "full_document")
    String fullDocument;
    @JSONField(name = "table_name")
    String tableName;
    @JSONField(name = "event_time")
    Long eventTime;

    @Override
    public String toString() {
        return "MongoCdcCotent{" +
                "documentKey='" + documentKey + '\'' +
                ", operationType='" + operationType + '\'' +
                ", fullDocument='" + fullDocument + '\'' +
                ", tableName='" + tableName + '\'' +
                ", eventTime=" + eventTime +
                '}';
    }

    public String toJsonString() {
        return JSONObject.toJSONString(this);
    }
}
