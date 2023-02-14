package com.flink.cdc.function;

import com.alibaba.fastjson.JSONObject;
import com.flink.sink.kudu.utils.RowSerializable;
import org.apache.flink.api.common.functions.MapFunction;
import org.joda.time.DateTime;
import org.slf4j.LoggerFactory;


/**
 * description: MongoCdc2KuduMap <br>
 * date: 2022-11-17 18:28 <br>
 * author: YQ <br>
 * version: 1.0 <br>
 */

public class MongoCdc2KuduMap implements MapFunction<String, RowSerializable> {
    private static org.slf4j.Logger LOG = LoggerFactory.getLogger(MongoCdc2KuduMap.class);

    @Override
    public RowSerializable map(String mongoCdc) throws Exception {
        RowSerializable rowSerializable = new RowSerializable(7);
        JSONObject mongoCdcEvent = JSONObject.parseObject(mongoCdc);

        String ds = new DateTime(mongoCdcEvent.getLong("event_time")).toString("yyyyMMdd");
        rowSerializable.setField(0, mongoCdcEvent.getString("document_key"));
        rowSerializable.setField(1, mongoCdcEvent.getString("table_name"));
        rowSerializable.setField(2, ds);
        rowSerializable.setField(3, mongoCdcEvent.getString("operation_type"));
        rowSerializable.setField(4, mongoCdcEvent.getString("full_document"));
        rowSerializable.setField(5, mongoCdcEvent.getLong("event_time"));
        rowSerializable.setField(6, System.currentTimeMillis());

        return rowSerializable;
    }
}
