package com.flink.cdc.deserializer;


import com.alibaba.fastjson.JSONObject;
import com.flink.cdc.bean.MongoCdcCotent;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import net.logstash.logback.encoder.org.apache.commons.lang.StringUtils;
import org.apache.curator.shaded.com.google.common.base.Strings;
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
import org.slf4j.LoggerFactory;

/**
 * description: MongoRecordDeserializer <br>
 * date: 2022-11-24 15:28 <br>
 * author: YQ <br>
 * version: 1.0 <br>
 */
public class MongoRecordDeserializer implements DebeziumDeserializationSchema<String> {
    private static final long serialVersionUID = 111L;
    private static org.slf4j.Logger LOG = LoggerFactory.getLogger(MongoRecordDeserializer.class);

    public MongoRecordDeserializer() {

    }

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
        //完整文档内容
        String fullDocument = "";
        //文档key
        String documentKey = "";
        String tableName = "";
        //namespace,包含库名和表名
        Struct ns = value.getStruct("ns");
        //操作类型
        String operationType = value.getString("operationType");
        //更新时间
        Long eventTime = value.getStruct("source").getInt64("ts_ms");

        //发生删除操作时，没有fullDocument会报空指针异常
        if (StringUtils.isNotEmpty(value.getString("fullDocument"))) {
            fullDocument = Document.parse(value.getString("fullDocument")).toJson(settings);
        }

        //发生集合删除时，没有documentKey会报空指针异常
        if (StringUtils.isNotEmpty(value.getString("documentKey"))) {
            documentKey = Document.parse(value.getString("documentKey")).toJson(settings);
            documentKey = JSONObject.parseObject(documentKey).getString("_id");

        }

        if (StringUtils.isNotEmpty(value.getStruct("ns").toString())) {
            tableName = ns.getString("db") + "." + ns.getString("coll");
        }

        /*if ("maybe.mb_single_chat_message".equals(tableName)
                && StringUtils.isNotEmpty(documentKey)
                && (!"delete".equals(operationType))
        )
        {
            LOG.info("document_key is :"+documentKey+" ,event_time is :"+eventTime+",operation_type is :"+operationType);
        }*/


        if (StringUtils.isNotEmpty(documentKey) && StringUtils.isNotEmpty(tableName)) {
            collector.collect(
                    MongoCdcCotent
                            .builder()
                            .documentKey(documentKey)
                            .fullDocument(Strings.nullToEmpty(fullDocument))
                            .operationType(operationType)
                            .tableName(tableName)
                            .eventTime(eventTime)
                            .build()
                            .toJsonString()
            );
        }
    }


    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
