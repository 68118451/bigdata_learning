package com.flink.cdc.deserializer;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.bson.Document;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;
import org.bson.json.StrictJsonWriter;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;

public class MongoToKafkaDeserializer implements DebeziumDeserializationSchema<String> {
    private static final long serialVersionUID = 111L;

    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) {

        JsonWriterSettings settings = JsonWriterSettings.builder()
                .outputMode(JsonMode.RELAXED)
                .objectIdConverter((ObjectId value, StrictJsonWriter writer) -> writer.writeString(String.valueOf(value)))
                .int64Converter((Long value, StrictJsonWriter writer) -> writer.writeString(Long.toString(value)))
                .int32Converter((Integer value, StrictJsonWriter writer) -> writer.writeNumber(Integer.toString(value)))
                .doubleConverter((Double value, StrictJsonWriter writer) -> writer.writeString(Double.toString(value)))
                .dateTimeConverter((Long value, StrictJsonWriter writer) -> writer.writeString(Long.toString(value)))
                .decimal128Converter((Decimal128 value, StrictJsonWriter writer) -> writer.writeString(value.toString()))
                .build();


        Struct value = (Struct) sourceRecord.value();

        //文档key
        String documentKey = "";
        //发生集合删除时，没有documentKey会报空指针异常
        if (value.getString("documentKey") != null) {
            //这里获取的是json格式
            documentKey = Document.parse(value.getString("documentKey")).toJson(settings);
        }
        JSONObject key = JSONObject.parseObject(documentKey);


        //获取事件时间
        long eventTime = 0L;
        if (value.getStruct("source") != null) {
            eventTime = value.getStruct("source").getInt64("ts_ms");
        }

        //获取fullDocument
        String fullDocument = "";
        if (value.getString("fullDocument") != null) {
            fullDocument = Document.parse(value.getString("fullDocument")).toJson(settings);
        }


        //获取处理时间
        long prossTime = 0L;
        if (value.get("ts_ms") != null) {
            prossTime = Long.parseLong(value.get("ts_ms").toString());
        }


        //1.创建 JSON 对象用于存储最终数据
        JSONObject result = new JSONObject();

        //2.获取库名&表名放入 source

        String topic = sourceRecord.topic();
        String[] fields = topic.split("\\.");
        String database = "";
        String tableName = "";
        if (fields.length >= 2) {
            database = fields[0];
            tableName = fields[1];
        }


        //拼接source字段
        JSONObject source = new JSONObject();
        source.put("db", database);
        source.put("table", tableName);
        source.put("ts_ms", eventTime);
        source.put("key", key);



        //获取操作类型 CREATE UPDATE DELETE 进行符合 Debezium-op 的字母
        String operationType = value.getString("operationType");
        String type = operationType.toLowerCase();
        if ("insert".equals(type)) {
            type = "c";
        }
        if ("update".equals(type)) {
            type = "u";
        }
        if ("delete".equals(type)) {
            type = "d";
        }
        if ("create".equals(type)) {
            type = "c";
        }
        if ("read".equals(type)) {
            type = "r";
        }
        if ("replace".equals(type)) {
            type = "u";
        }

        //对delete事件和update事件做处理
        // delete事件和update事件的before赋值主键,update事件的after赋值为null
        JSONObject beforeJson = new JSONObject();
        ;
        JSONObject afterJson = JSONObject.parseObject(fullDocument);
        if ("d".equals(type)) {
            beforeJson = JSONObject.parseObject(documentKey);
            afterJson = new JSONObject();
        }
        if ("u".equals(type)) {
            beforeJson = JSONObject.parseObject(documentKey);
        }


        //6.将字段写入 JSON 对象
        result.put("source", source);
        result.put("before", beforeJson);
        result.put("after", afterJson);
        result.put("op", type);
        result.put("ts_ms", prossTime);


        //7.输出数据
        collector.collect(result.toJSONString());
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }

}
