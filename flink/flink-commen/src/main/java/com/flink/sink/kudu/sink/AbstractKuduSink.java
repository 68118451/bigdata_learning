package com.flink.sink.kudu.sink;


import com.flink.conf.CommonConf;
import com.flink.sink.kudu.conf.KuduOption;
import com.flink.sink.kudu.type.KuduOperatorType;
import com.flink.sink.kudu.utils.RowSerializable;
import com.flink.sink.kudu.utils.TableUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author fanrui
 * @time 2019-09-24 11:04:52
 */
public abstract class AbstractKuduSink extends RichSinkFunction<RowSerializable> implements CheckpointedFunction {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractKuduSink.class);

    private transient long kuduSinkExecutionMills;

    private int flushBufferSize = 0;
    protected transient KuduClient client;
    protected CommonConf commonConf = new CommonConf();

    /**
     * KuduSession is not thread-safe.
      */
    private transient KuduSession session;
    protected transient KuduTable table;
    protected String[] fieldsNames;

    private KuduOption kuduOption;
    protected String tableName;
    private KuduOperatorType operatorType;

    private transient Object lock;

    private transient ScheduledExecutorService timer;


    /**
     * kudu sink connector
     *
     * @param tableName           kudu table name
     * @param operatorType        insert upsert
     */
    public AbstractKuduSink(String tableName, KuduOperatorType operatorType, KuduOption kuduOption) {

        if (tableName == null || tableName.isEmpty()) {
            throw new IllegalArgumentException("ERROR: Param \"tableName\" not valid (null or empty)");
        }

        this.kuduOption = kuduOption;
        this.tableName = tableName;
        this.operatorType = operatorType;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // init lock
        lock = new Object();
        this.client = new KuduClient.KuduClientBuilder(kuduOption.getMasterAddressList())
                .bossCount(kuduOption.getMaxBossThreadNum())
                .defaultOperationTimeoutMs(kuduOption.getOperationTimeoutMs())
                .defaultSocketReadTimeoutMs(kuduOption.getSocketReadTimeoutMs())
                .workerCount(kuduOption.getMaxWorkerThreadNum())
                .build();
        this.table = client.openTable(tableName);

        // KuduSession is not thread-safe.
        this.session = client.newSession();
        this.session.setMutationBufferSpace(kuduOption.getMutationBufferSize());
        this.session.setTimeoutMillis(kuduOption.getTimeoutMs());
        this.session.setIgnoreAllDuplicateRows(kuduOption.isIgnoreAllDuplicateRows());
        this.session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);

        fieldsNames = getNamesOfColumns(table);

        getRuntimeContext().getMetricGroup().gauge("kudu-sink-execution-mills", (Gauge<Long>) () -> kuduSinkExecutionMills);
        timer = new ScheduledThreadPoolExecutor(1);
        timer.scheduleAtFixedRate(() -> {
            try {
                flushKudu();
            } catch (KuduException e) {
                LOG.error("Flush kudu fail, tableName:{}, msg:{}", tableName, e.getMessage());
            }
        }, kuduOption.getFlushPeriod(), kuduOption.getFlushPeriod(), TimeUnit.MILLISECONDS);
    }


    protected final void invokeData(RowSerializable row) throws KuduException {
        if (row == null) {
            return;
        }
        switch (operatorType) {
            case INSERT_MODE: putData2Session(table.newInsert(), row); break;
            case UPSERT_MODE: putData2Session(table.newUpsert(), row); break;
            default:
        }
        flushBufferSize++;
        if (flushBufferSize >= kuduOption.getBatchSize()) {
            flushKudu();
        }
    }


    private void putData2Session(Operation operation, RowSerializable row) throws KuduException {
        for (int index = 0; index < row.productArity(); index++) {
            // todo 这里如果为null，已经continue了，为什么当某个字段为null时，type还会报空指针
            if (Objects.isNull(row.productElement(index))) {
                continue;
            }
            Type type = TableUtils.mapToType(row.productElement(index).getClass());
            PartialRow partialRow = operation.getRow();
            TableUtils.valueToRow(partialRow, type, fieldsNames[index], row.productElement(index));
        }
        session.apply(operation);
    }


    private String[] getNamesOfColumns(KuduTable table) {
        List<ColumnSchema> columns = table.getSchema().getColumns();
        //  List of column names
        List<String> columnsNames = new ArrayList<>();
        for (ColumnSchema schema : columns) {
            columnsNames.add(schema.getName());
        }
        String[] array = new String[columnsNames.size()];
        array = columnsNames.toArray(array);
        return array;
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        flushKudu();
    }


    @Override
    public void close() throws Exception {
        sessionClose();
    }


    protected final void flushKudu() throws KuduException {
        if (flushBufferSize > 0 && client != null && table != null && session != null) {
            long i = System.nanoTime();
            synchronized (lock) {
                session.flush();
            }
            // gauge with flink metrics system
            kuduSinkExecutionMills = (System.nanoTime() - i) / 1000000;
            flushBufferSize = 0;
        }
    }


    private final void sessionClose() throws IOException {
        if (client != null && table != null && session != null) {
            synchronized (lock) {
                session.flush();
                session.close();
            }
            this.client.close();
        }
    }

}

