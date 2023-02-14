package com.flink.sink.kudu.sink;


import com.flink.sink.kudu.conf.KuduOption;
import com.flink.sink.kudu.type.KuduOperatorType;
import com.flink.sink.kudu.utils.RowSerializable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author fanrui
 * @time 2019-09-24 15:14:48
 */
public class KuduSink extends AbstractKuduSink {

    private static final Logger LOG = LoggerFactory.getLogger(KuduSink.class);

    /**
     * kudu sink connector
     * @param tableName           kudu table name
     * @param operatorType        insert upsert
     */
    public KuduSink(String tableName, KuduOperatorType operatorType, KuduOption kuduOption) {
        super(tableName,operatorType,kuduOption);
    }


    @Override
    public void invoke(RowSerializable row, Context context) throws Exception {
        invokeData(row);
    }


}

