package com.flink.sink.kudu.conf;

import org.apache.commons.lang3.Validate;

import java.io.Serializable;

public class KuduOption implements Serializable {

    private static final long DEFAULT_OPERATION_TIMEOUT_MS = 60000;
    private static final long DEFAULT_SOCKET_READ_TIMEOUT_MS = 60000;
    private static final int DEFAULT_MAX_WORKER_THREAD_NUM = 1;
    private static final int DEFAULT_MAX_BOSS_THREAD_NUM = 1;
    private static final boolean DEFAULT_IGNORE_ALL_DUPLICATE_ROWS = true;
    private static final long DEFAULT_TIMEOUT_MILLIS = 30000;
    private static final int DEFAULT_BATCH_SIZE = 5000;
    private static final int DEFAULT_MUTATION_BUFFER_SIZE = 200000;
    private static final int DEFAULT_FLUSH_PERIOD = 3000;


    private String masterAddressList;

    private long operationTimeoutMs = DEFAULT_OPERATION_TIMEOUT_MS;

    private long socketReadTimeoutMs = DEFAULT_SOCKET_READ_TIMEOUT_MS;

    private int maxWorkerThreadNum = DEFAULT_MAX_WORKER_THREAD_NUM;

    private int maxBossThreadNum = DEFAULT_MAX_BOSS_THREAD_NUM;

    private int batchSize = DEFAULT_BATCH_SIZE;

    private boolean ignoreAllDuplicateRows = DEFAULT_IGNORE_ALL_DUPLICATE_ROWS;

    private long timeoutMs = DEFAULT_TIMEOUT_MILLIS;

    private int mutationBufferSize = DEFAULT_MUTATION_BUFFER_SIZE;

    /**
     * 基于时间的 flush 策略，单位是 ms
     */
    private long flushPeriod = DEFAULT_FLUSH_PERIOD;


    public String getMasterAddressList() {
        return masterAddressList;
    }

    public KuduOption setMasterAddressList(String masterAddressList) {
        this.masterAddressList = masterAddressList;
        return this;
    }

    public long getOperationTimeoutMs() {
        return operationTimeoutMs;
    }

    public KuduOption setOperationTimeoutMs(long operationTimeoutMs) {
        this.operationTimeoutMs = operationTimeoutMs;
        return this;
    }

    public long getSocketReadTimeoutMs() {
        return socketReadTimeoutMs;
    }

    public KuduOption setSocketReadTimeoutMs(long socketReadTimeoutMs) {
        this.socketReadTimeoutMs = socketReadTimeoutMs;
        return this;
    }


    public int getMaxWorkerThreadNum() {
        return maxWorkerThreadNum;
    }

    public KuduOption setMaxWorkerThreadNum(int maxWorkerThreadNum) {
        this.maxWorkerThreadNum = maxWorkerThreadNum;
        return this;
    }

    public int getMaxBossThreadNum() {
        return maxBossThreadNum;
    }

    public KuduOption setMaxBossThreadNum(int maxBossThreadNum) {
        this.maxBossThreadNum = maxBossThreadNum;
        return this;
    }

    public KuduOption setBatchSize(int batchSize) {
        Validate.isTrue(batchSize >= 1, "batchSize must > 0");
        this.batchSize = batchSize;
        return this;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public long getFlushPeriod() {
        return flushPeriod;
    }

    public KuduOption setFlushPeriod(long flushPeriod) {
        Validate.isTrue(flushPeriod >= 200, "flushPeriod must >= 200");
        this.flushPeriod = flushPeriod;
        return this;
    }

    public KuduOption ignoreAllDuplicateRows(boolean ignoreAllDuplicateRows) {
        this.ignoreAllDuplicateRows = ignoreAllDuplicateRows;
        return this;
    }

    public KuduOption setTimeoutMs(long timeoutMs) {
        this.timeoutMs = timeoutMs;
        return this;
    }

    public KuduOption setMutationBufferSpaceSize(int mutationBufferSpaceSize) {
        this.mutationBufferSize = mutationBufferSpaceSize;
        return this;
    }

    public boolean isIgnoreAllDuplicateRows() {
        return ignoreAllDuplicateRows;
    }

    public long getTimeoutMs() {
        return timeoutMs;
    }

    public int getMutationBufferSize() {
        return mutationBufferSize;
    }
}