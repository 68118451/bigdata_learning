package com.flink.cdc.function;

import com.alibaba.fastjson.JSONObject;
import com.flink.sink.kudu.utils.RowSerializable;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.joda.time.DateTime;
import org.slf4j.LoggerFactory;


/**
 * description: MongoCdcJson2KuduMap <br>
 * date: 2023-01-12 11:56 <br>
 * author: YQ <br>
 * version: 1.0 <br>
 */
public class MongoCdcJson2KuduMap implements MapFunction<String, RowSerializable> {
    private static org.slf4j.Logger LOG = LoggerFactory.getLogger(MongoCdcJson2KuduMap.class);

    @Override
    public RowSerializable map(String mongoCdc) throws Exception {

        RowSerializable rowSerializable = new RowSerializable(7);

        JSONObject mongoCdcEvent = JSONObject.parseObject(mongoCdc);

        Long eventTime=mongoCdcEvent.getJSONObject("source").getLong("ts_ms");
        String tableName=mongoCdcEvent.getJSONObject("ns").getString("db")+"."+mongoCdcEvent.getJSONObject("ns").getString("coll");
        String operationType=mongoCdcEvent.getString("operationType");
        String documentKey=mongoCdcEvent.getJSONObject("documentKey").getJSONObject("_id").getString("$numberLong");

        LOG.info("documentKey:"+documentKey+",operationType:"+operationType+",event_time"+eventTime);


        if (StringUtils.isNotEmpty(documentKey) && StringUtils.isNotEmpty(tableName)){
            String ds = new DateTime(eventTime).toString("yyyyMMdd");
            rowSerializable.setField(0, documentKey);
            rowSerializable.setField(1, tableName);
            rowSerializable.setField(2, ds);
            rowSerializable.setField(3, operationType);
            rowSerializable.setField(4, mongoCdcEvent.getString("fullDocument"));
            rowSerializable.setField(5, eventTime);
            rowSerializable.setField(6, System.currentTimeMillis());
        }
        return rowSerializable;
    }
}
