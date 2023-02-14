package com.flink.cdc.deserializer;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;


public class MysqlToKafkaDeserializer implements DebeziumDeserializationSchema<String> {


    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {

        Struct value = (Struct) sourceRecord.value();

        //1.创建 JSON 对象用于存储最终数据
        JSONObject result = new JSONObject();

        //2.获取库名&表名放入 source
        String topic = sourceRecord.topic();
        String[] fields = topic.split("\\.");
        String database = fields[1];
        String tableName = fields[2];
        JSONObject source = new JSONObject();
        //获取事件时间
        Struct source2 = value.getStruct("source");
        long event_time = Long.parseLong(source2.get("ts_ms").toString());

        //获取表的主键
        Struct key = (Struct) sourceRecord.key();
        JSONObject keyJson = new JSONObject();
        if (key != null) {
            Schema keySchema = key.schema();
            List<Field> beforeFields = keySchema.fields();
            for (Field field : beforeFields) {
                Object beforeValue = key.get(field);
                keyJson.put(field.name(), beforeValue);
            }
        }

        source.put("db", database);
        source.put("table", tableName);
        source.put("ts_ms", event_time);
        source.put("key", keyJson);


        //3.获取"before"数据
        Struct before = value.getStruct("before");
        JSONObject beforeJson = new JSONObject();
        if (before != null) {
            Schema beforeSchema = before.schema();
            List<Field> beforeFields = beforeSchema.fields();
            for (Field field : beforeFields) {
                Object beforeValue = before.get(field);
                beforeJson.put(field.name(), beforeValue);
            }
        }

        //4.获取"after"数据
        Struct after = value.getStruct("after");
        JSONObject afterJson = new JSONObject();
        if (after != null) {
            Schema afterSchema = after.schema();
            List<Field> afterFields = afterSchema.fields();
            for (Field field : afterFields) {
                Object afterValue = after.get(field);
                afterJson.put(field.name(), afterValue);
            }
        }

        //获取处理时间
        long pross_time = Long.parseLong(value.get("ts_ms").toString());
//        String dt = MyDateUtils.getTimestamp2Fm(ts);

        //5.获取操作类型 CREATE UPDATE DELETE 进行符合 Debezium-op 的字母
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
        String type = operation.toString().toLowerCase();
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

        //6.将字段写入 JSON 对象
        result.put("source", source);
        result.put("before", beforeJson);
        result.put("after", afterJson);
        result.put("op", type);
        result.put("ts_ms", pross_time);

        //7.输出数据
        collector.collect(result.toJSONString());

    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}

