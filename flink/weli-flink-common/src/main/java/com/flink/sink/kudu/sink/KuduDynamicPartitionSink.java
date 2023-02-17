package com.flink.sink.kudu.sink;


import com.flink.sink.kudu.conf.KuduOption;
import com.flink.sink.kudu.type.KuduOperatorType;
import com.flink.sink.kudu.utils.RowSerializable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.transaction.CuratorTransactionResult;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.accumulators.LongMaximum;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.kudu.client.AlterTableOptions;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.PartialRow;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent.Type.CHILD_ADDED;

/**
 * @author fanrui
 * @date 2019-09-24 10:56:32
 * desc
 * znode 设计：
 * root znode 为 /weli/bigdata/flink-app/kudu-sink/ + tableName
 *
 * rootZnode/lock                           -- 分布式锁
 * rootZnode/subtask                        -- subtask 实例去该 znode 下去注册
 * rootZnode/subtask/0                      -- 0 号 subtask 注册的 znode
 * rootZnode/subtask/1
 * rootZnode/subtask/2
 * rootZnode/subtask/3
 * rootZnode/subtask/4
 * rootZnode/subtask/5
 * rootZnode/hour                           -- 所有要写入数据的小时
 * rootZnode/hour/2019092710                -- 小时分区 2019092710，data中保存 boolean 类型表示该小时是否允许再创建新分区
 *                                todo 这里虽然保存了 flag，但是并没有依据 zk，而是内容里有一份，后期深入考虑内容里的数据靠谱吗
 * rootZnode/hour/2019092710/201909271000   -- partition分区 201909271000，data中保存该 partition 目前写入了多少数据
 * rootZnode/hour/2019092710/201909271001
 * rootZnode/hour/2019092710/201909271002
 * rootZnode/hour/2019092711
 * rootZnode/hour/2019092711/201909271100
 * rootZnode/hour/2019092711/201909271101
 * rootZnode/hour/2019092711/201909271102
 * rootZnode/checkpoint                     -- checkpoint 目录保存CheckPoint 时的信息
 * rootZnode/checkpoint/100                 -- 第100次CheckPoint，data 存储当前 chk 执行的状态
 * rootZnode/checkpoint/100/0               -- 0 号 subtask 将counter 已经累加到 partition 的znode中
 * rootZnode/checkpoint/100/1
 * rootZnode/checkpoint/100/2
 * rootZnode/checkpoint/100/3
 * rootZnode/checkpoint/100/4
 * rootZnode/checkpoint/100/5
 * rootZnode/checkpoint/101
 * rootZnode/checkpoint/101/0
 * rootZnode/checkpoint/101/1
 * rootZnode/checkpoint/101/2
 * rootZnode/checkpoint/101/3
 * rootZnode/checkpoint/101/4
 * rootZnode/checkpoint/101/5
 *
 * todo 需要将当前对循环访问 zk 的 getChildren 和 getData 操作，优化为 Watcher 机制
 * 1. 创建 znode
 * 2. 删除过期的 znode
 * 3. 内存中过期的 hourPartitionMap 映射，也删除
 */
public class KuduDynamicPartitionSink extends AbstractKuduSink implements CheckpointListener {


    private static final Logger LOG = LoggerFactory.getLogger(KuduDynamicPartitionSink.class);

    /**
     * 每个 partition 默认允许接收的最大数据量
      */
    private static final long DEFAULT_DYNAMIC_PARTITION_MAX_RECORD = 200_000_000L;
    private static final long DYNAMIC_PARTITION_MAX_RECORD_MIN = 500000;
    private static final int ZK_SESSION_TIMEOUT_MS = 5 * 1000;

    /**
     * 每个 partition 默认允许接收多长时间的数据量
     */
    private static final long DEFAULT_DYNAMIC_PARTITION_TIMEOUT = Long.MAX_VALUE;
    private static final long DYNAMIC_PARTITION_TIMEOUT_MIN = TimeUnit.MINUTES.toMillis(5);

    private long dynamicPartitionMaxRecord = DEFAULT_DYNAMIC_PARTITION_MAX_RECORD;
    private long dynamicPartitionTimeout = DEFAULT_DYNAMIC_PARTITION_TIMEOUT;

    /**
     * 所有小时当前要写入的 partition 编号
     * key  : 2019070108    表示 2019年 7月 1日 8点
     * value: <true,201907010800>   left 为 Boolean 类型，true 表示当前小时还允许创建新分区， false 表示当前小时不允许创建新分区
     *                              right 表示 2019年 7月 1日 8点 正在写当前小时的第0号partition
     */
    private Map<Long, MutablePair<Boolean,Long>> hourPartitionMap;

    /**
     * 当前正在写数据的最新分区是属于哪个小时，例：2019070112
     * todo  这个数据应该需要保存在 znode 一份，保障全局统一，然后所有 Subtask Watch即可,目前直接watch hour znode 的 children，也能达到此功能
     */
    private LongMaximum currentHour;

    /**
     * 当前小时，当前写入数据的 partition 已经写入的总数据量
     * 该值每次从 partition 对应的 znode 中去恢复
     */
    private LongMaximum currentPartitionCounter;

    /**
     * 当前小时，当前 subtask 写入的数据增量，每次会把这个值 刷到 zk 中
     */
    private LongCounter currentSubTaskCounter;

    /**
     * 当前小时，最新的 partition 是从什么时间开始写入数据的
     * 正常情况下，创建相应 znode 的 create_time
     * 任务恢复时: 选取当前小时最后一个 znode 的 create_time
     */
    private LongMaximum currentPartitionTimestamp;

    /**
     * Kudu 表 range 分区字段的列名
     */
    private int rangePartitionIndex;
    private String rangePartitionColumnName;

    /**
     * Kudu 操作相关需要用到的分布式互斥锁
     */
    private CuratorFramework curatorFramework;
    private InterProcessMutex interProcessMutex;
    private String rootZnode;
    private String hourRootZnode;
    private String checkpointRootZnode;
    private String subtaskRootZnode;


    private static String DATE_FORMAT = "yyyyMMddHH";


    ListState<Long> currentPartitionState;


    /**
     * checkpoint 的时候， Leader 节点做了什么工作？
     */
    private enum CHECKPOINT_LEADER_STATUS {
        // 本次 checkpoint 的时候， Leader 节点创建了新 partition
        CREATE_NEW_PARTITION(1),

        // 本次 checkpoint 的时候， Leader 节点没有创建新 partition
        NOT_CREATE_NEW_PARTITION(2);

        private byte[] zkData;
        private int index;

        CHECKPOINT_LEADER_STATUS(int index){
            this.index = index;
            this.zkData = Ints.toByteArray(index);

        }


        public static CHECKPOINT_LEADER_STATUS indexOf(int index) {
            switch (index) {
                case 1: return CREATE_NEW_PARTITION;
                case 2: return NOT_CREATE_NEW_PARTITION;
                default: return NOT_CREATE_NEW_PARTITION;
            }
        }


        public byte[] getZkData() {
            return zkData;
        }


        public long getIndex() {
            return index;
        }
    }

    private KuduSinkStatus kuduSinkStatus;

    private enum KuduSinkStatus {
        /** Leader 表示 当前 subtask 是 KuduSink 的Leader，有些操作交给 Leader 来做
         * 例如： 创建 Kudu range partition ，清理没用的 znode
         */
        Leader,
        // Follower 表示 当前 subtask 是 KuduSink 的 Follower，有些操作 Follower 不做
        Follower
    }

    /**
     * znode 默认保留 24 小时
      */

    private static int ZNODE_TTL = 24;





    /**
     * kudu sink connector
     *
     * @param tableName           kudu table name
     * @param operatorType        insert upsert
     */
    public KuduDynamicPartitionSink(String tableName, KuduOperatorType operatorType, KuduOption kuduOption) {
        super(tableName,operatorType,kuduOption);
        this.rootZnode = "/weli/bigdata/flink-app/kudu-sink/" + tableName;
        this.subtaskRootZnode = rootZnode + "/subtask";
        this.checkpointRootZnode = rootZnode + "/checkpoint";
        this.hourRootZnode = rootZnode + "/hour";
    }


    /**
     *
     * @param dynamicPartitionMaxRecord 每个 partition 默认允许接收的最大数据量
     */
    public KuduDynamicPartitionSink setDynamicPartitionMaxRecord(long dynamicPartitionMaxRecord) {
        Validate.isTrue(dynamicPartitionMaxRecord >= DYNAMIC_PARTITION_MAX_RECORD_MIN
                ,"dynamicPartitionMaxRecord must > " + DYNAMIC_PARTITION_MAX_RECORD_MIN);
        this.dynamicPartitionMaxRecord = dynamicPartitionMaxRecord;
        return this;
    }


    /**
     *
     * @param dynamicPartitionTimeout 每个 partition 默认允许接收多长时间的数据量，或者说是每个 partition 的最长存活时间
     *                                单位是 ms
     */
    public KuduDynamicPartitionSink setDynamicPartitionTimeout(long dynamicPartitionTimeout, TimeUnit timeUnit) {
        Validate.isTrue(timeUnit.toMillis(dynamicPartitionTimeout) >= DYNAMIC_PARTITION_TIMEOUT_MIN
                ,"dynamicPartitionTimeout 换算成毫秒 must > " + DYNAMIC_PARTITION_TIMEOUT_MIN);
        this.dynamicPartitionTimeout = timeUnit.toMillis(dynamicPartitionTimeout);
        return this;
    }


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        // 恢复 currentPartitionCounter 和 currentPartitionTimestamp
        currentSubTaskCounter = new LongCounter();
        currentPartitionCounter = new LongMaximum(0);
        currentPartitionTimestamp = new LongMaximum(0);
        currentHour = new LongMaximum(0);

        initRangePartitionColumnName();

        if(0 == getRuntimeContext().getIndexOfThisSubtask()){
            kuduSinkStatus = KuduSinkStatus.Leader;
        } else {
            kuduSinkStatus = KuduSinkStatus.Follower;
        }

        initCuratorFrameworkAndInterProcessMutex();

        // create 功能性的 znode，例如 hour  subtask  checkpoint
        checkAndCreateBaseRootZnode();

        /* 所有的并行度根据当前自己机器的时间戳 都去 zk 下检查当前小时以及当前小时之前是否都创建了相应的 znode
            如果没有对应的 znode，则为其创建 znode，这块需要两级 znode ，一个是 小时级别，一个是小时下的partition级别
            并把这些小时的 znode setDate = false ，不允许再创建子节点
            这种方案也解决了 当任务重启的时间恰好为整小时的交界处，
            有些并行度可能为 上一个小时，有些为下一个小时 的情况下，我们会创建下一个小时的znode
            注：这里不需要创建 kudu range 分区，每个小时的 0 号 partition 由离线任务去创建
         */
        checkAndCreateMissZnode();

        // 所有的 Subtask 实例都去zk注册，create 一个临时 znode，等待所有的 SubTask 实例注册完成
        registerAndAwaitAllSubtaskAlive();

        // 必须等所有的节点都创建完 znode 后，再去恢复 currentHour 和 hourPartitionMap
        restoreCurrentHourAndHourPartitionMap();

        new HourZnodeWatcher().start();
    }


    /**
     * 监听 增加小时 znode，如果增加 修改各 Counter
     */
    private class HourZnodeWatcher extends Thread {

        @Override
        public void run() {
            PathChildrenCache hourNodeCache = new PathChildrenCache(curatorFramework, hourRootZnode, false);
            int beginIndex = hourRootZnode.length() + 1;
            try {
                hourNodeCache.start();
                hourNodeCache.getListenable().addListener((zkClient, event) -> {
                    if (CHILD_ADDED.equals(event.getType())){
                        long children = NumberUtils.toLong(event.getData().getPath().substring(beginIndex));
                        if (children > currentHour.getLocalValue()) {
                            LOG.info("HourZnodeWatcher, eventType: {}  eventData:{}", event.getType(), event.getData());
                            currentHour.add(children);
                            long hourLatestPartition = getHourLatestPartition(event.getData().getPath());
                            hourPartitionMap.put(currentHour.getLocalValue(), MutablePair.of(true, hourLatestPartition));
                            switchNewPartitionResetCounter(hourLatestPartition);
                            if(KuduSinkStatus.Leader.equals(kuduSinkStatus)){
                                currentPartitionState.clear();
                                currentPartitionState.add(hourLatestPartition);
                            }
                        }
                    }
                });
            } catch (Exception e) {
                LOG.error("HourZnodeWatcher failed.", e);
            }
        }
    }

    @Override
    public void invoke(RowSerializable row, Context context) throws Exception {

        Object hour = row.productElement(rangePartitionIndex);
        if (hour instanceof Long) {
            if (currentHour.getLocalValue().equals(hour)) {
                // 当前小时的数据，计数 +1
                currentSubTaskCounter.add(1);
            } else if (currentHour.getLocalValue().compareTo( (Long) hour) < 0) {
                // 当前小时 小于 数据里的小时,则创建相应的 znode
                initHourZnode((Long) hour, true);
            }
            MutablePair<Boolean, Long> booleanLongMutablePair = hourPartitionMap.getOrDefault(hour,
                    MutablePair.of(true, (Long) hour * 100));
            row.setField(rangePartitionIndex, booleanLongMutablePair.right);
            invokeData(row);
        } else {
            LOG.warn("kudu range 分区字段必须填写 long 类型数据，且 格式为:" + DATE_FORMAT);
            throw new RuntimeException("kudu range 分区字段必须填写 long 类型数据，且 格式为:" + DATE_FORMAT);
        }

    }


    /**
     * 检测当前是否需要新建分区，如果需要，则创建
     * @param context context
     * @throws Exception zk
     */
    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        super.snapshotState(context);

        // 所有内存里的 counter 应该累加到 zk partition znode的 data 中, 累加结束后，将节点注册到 chk 的 znode下
        String registerPath = checkpointRootZnode + "/" + context.getCheckpointId();
        accumulateCounter2ZnodeAndRegister(registerPath);

        MutablePair<Boolean, Long> hourPartition = hourPartitionMap.get(currentHour.getLocalValue());
        // 为了减少分布式锁的问题，只有 Leader 的subtask 才有权利去创建新 partition 对应的 znode
        if ( KuduSinkStatus.Leader.equals(kuduSinkStatus) ) {
            // 等待所有的 subtask 累加 Counter 结束
            awaitAllSubtaskRegister(registerPath, TimeUnit.SECONDS, 30);
            // 从zk中获取当前 partition 对应的 counter
            byte[] currentPartitionCounterBytes = curatorFramework.getData()
                                                    .forPath(getCurrentHourPartitionPath());
            currentPartitionCounter.add(Longs.fromByteArray(currentPartitionCounterBytes));
            // 当前小时允许创建新 Partition，则创建新的 partition 和相应znode
            if ( isCreateNewPartition() ) {
                LOG.info("prepare create new partition. current partition is {}, current partition {} counter is {}",
                        new Object[]{currentHour.getLocalValue(), hourPartitionMap.get(currentHour.getLocalValue()).right,
                        currentPartitionCounter.getLocalValue()});
                createRangePartitionAndZnode(registerPath);
            } else {
                curatorFramework.setData().forPath(registerPath, CHECKPOINT_LEADER_STATUS.NOT_CREATE_NEW_PARTITION.getZkData());
            }
            deleteChildren(subtaskRootZnode);
        }

        // 所有的 subtask 等待 Leader 执行完本次的 createRangePartitionAndZnode 操作
        CHECKPOINT_LEADER_STATUS checkpointLeaderStatus = awaitLeaderCreatePartition(registerPath, TimeUnit.SECONDS, 30);

        if( CHECKPOINT_LEADER_STATUS.CREATE_NEW_PARTITION.equals(checkpointLeaderStatus)){
            // 比较 znode 保存的 partition 与 hourPartitionMap保存的 partition 是否相等
            // todo 这里有可能为 10 点创建了一个新 partition，但是由于时间切换，突然切换到 11 点，
            // 可能会导致 这里获取 11 点的新分区，这里可能会数据不一致，有的节点 10点，有的节点 11 点
            long znodePartition = getHourLatestPartition(hourRootZnode + "/" + currentHour.getLocalValue());
            // 不等说明当前小时增加了新的分区
            if ( hourPartition.getRight() != znodePartition ) {
                awaitDiscoveryNewPartition(znodePartition);
                hourPartition.setRight(znodePartition);
                switchNewPartitionResetCounter(znodePartition);
            } else {
                throw new RuntimeException("Leader 创建了新partition，但是 内存中跟 zk 中保存的 新分区仍然一样都是" + znodePartition);
            }
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        super.initializeState(context);

        currentPartitionState = context.getOperatorStateStore().getUnionListState(
                new ListStateDescriptor<>("currentPartitionState", TypeInformation.of(new TypeHint<Long>() {
                })));
        if (getRuntimeContext().getIndexOfThisSubtask() != 0){
            currentPartitionState.clear();
        } else {
            Iterator<Long> iterator = currentPartitionState.get().iterator();
            while (iterator.hasNext()) {
                long currentPartition = iterator.next();
                LOG.info("Leader currentPartitionState : {}", currentPartition);
            }
        }
    }

    /**
     *  subtask 等待 发现kudu新的partition，如果没有此操作会丢数据
     * @param znodePartition kudu range partition
     */
    private void awaitDiscoveryNewPartition(long znodePartition) {

        int checkCounter = 0;
        while (!isExistsRangePartition(znodePartition)){
            checkCounter++;
            LOG.warn("subtask:{} 检测了 {} 次  未检测到 Kudu 分区：{}",
                    new Object[]{getRuntimeContext().getIndexOfThisSubtask(), checkCounter, znodePartition});
            if( checkCounter % 2 == 0){
                createRangePartition(rangePartitionColumnName, znodePartition);
            }
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                LOG.error("Occurred exception when awaitDiscoveryNewPartition.", e);
            }

            if( checkCounter > 30 ){
                throw new RuntimeException("subtask:" + getRuntimeContext().getIndexOfThisSubtask() + " 检测了 " +
                        checkCounter + " 次  未检测到 Kudu 分区：" + znodePartition);
            }
        }

    }


    /**
     * 切换新 分区后，将相关的 Counter 清零
     * @param znodePartition 新的 znodePartition 编号
     * @throws Exception zk
     */
    private void switchNewPartitionResetCounter(long znodePartition) throws Exception {
        currentSubTaskCounter.resetLocal();
        if( KuduSinkStatus.Leader.equals(kuduSinkStatus) ) {
            currentPartitionCounter.resetLocal();
            Stat stat = new Stat();
            curatorFramework.getData()
                    .storingStatIn(stat)
                    .forPath(hourRootZnode + "/" + currentHour.getLocalValue() + "/" + znodePartition);
            currentPartitionTimestamp.add(stat.getCtime());
        }
    }


    /**
     * 等待 znodePath 对应 znode 中 data 不为空
     * @param znodePath znodePath
     * @return CHECKPOINT_LEADER_STATUS CHECKPOINT_LEADER_STATUS
     */
    private CHECKPOINT_LEADER_STATUS awaitLeaderCreatePartition(String znodePath, TimeUnit timeUnit, long timeout) throws Exception {
        long startTs = System.currentTimeMillis();
        long timeoutMs = timeUnit.toMillis(timeout);

        AtomicInteger znodeIntData = new AtomicInteger(0);
        NodeCache nodeCache = new NodeCache(curatorFramework, znodePath, false);
        nodeCache.start(true);
        nodeCache.getListenable().addListener(() -> {
            byte[] data = nodeCache.getCurrentData().getData();
            if( data.length > 0) {
                znodeIntData.set(Ints.fromByteArray(data));
            }
        });

        byte[] znodeData = nodeCache.getCurrentData().getData();
        if( znodeData.length > 0) {
            znodeIntData.set(Ints.fromByteArray(znodeData));
        }

        // 等待指定的 znode 中数据不为空
        while (znodeIntData.get() == 0) {
            Thread.sleep(5);
            if(System.currentTimeMillis() - startTs > timeoutMs){
                throw new RuntimeException("subtask:" + getRuntimeContext().getIndexOfThisSubtask()
                        + "   awaitLeaderCreatePartition " + znodePath + " data change not null timeout");
            }
        }
        nodeCache.close();
        LOG.info("awaitLeaderCreatePartition {} status is {}", znodePath,
                CHECKPOINT_LEADER_STATUS.indexOf(znodeIntData.get()));
        return CHECKPOINT_LEADER_STATUS.indexOf(znodeIntData.get());
    }


    /**
     * 将内存里记录的当前 partition 写入的数据量 累加到 znode 记录的 counter 中
     * 累加后将 内存中的 counter 清零，并注册到 zk 的CheckPoint znode 下，表示自己已经累加结束
     * @param registerPath 注册 znode 的path
     */
    private void accumulateCounter2ZnodeAndRegister(String registerPath) throws Exception {

        // 累加操作
        while (currentSubTaskCounter.getLocalValue() > 0) {
            try {
                // 防止多并行度频繁访问 zk，所以加随机延迟
                Thread.sleep(new Random().nextInt(20));
                // 拿到 zk 中保存的 counter 值，加上内存中的 counter
                Stat currentHourPartitionStat = new Stat();
                byte[] bytes = curatorFramework.getData()
                        .storingStatIn(currentHourPartitionStat)
                        .forPath(getCurrentHourPartitionPath());

                long targetCounter = Longs.fromByteArray(bytes) + currentSubTaskCounter.getLocalValue();

                // 使用 zk 的 withVersion 进行 setData 操作，保障数据计算正确
                curatorFramework.setData()
                        .withVersion(currentHourPartitionStat.getVersion())
                        .forPath(getCurrentHourPartitionPath(), Longs.toByteArray(targetCounter));
                currentSubTaskCounter.resetLocal();
            } catch (Exception e) {
                LOG.error("Occurred exception when accumulateCounter2ZnodeAndRegister.", e);
            }
        }

        // register
        registerZnode(registerPath);
    }


    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        // todo Leader 去删除无用的znode
        if ( KuduSinkStatus.Leader.equals(kuduSinkStatus)){
            // 删除 1000 次之前的 checkpoint 数据
            recursionDelete(checkpointRootZnode + "/" + (checkpointId-1000));
            if(new Random().nextInt(1000) > 10 ){
                return;
            }
            for (Long hour:getHourZnodeBefore24Hour()){
                recursionDelete( hourRootZnode + "/" + hour);
            }
        }
    }


    /**
     * 递归删除指定目录
     * @param znodePath znodePath
     */
    private void recursionDelete(String znodePath) throws Exception {
        Stat stat = curatorFramework.checkExists().forPath(znodePath);
        // stat == null 表示 没有相应 znode
        if ( null == stat) {
            return;
        }
        for(String children :curatorFramework.getChildren().forPath(znodePath)){
            recursionDelete(znodePath + "/" + children);
        }
        curatorFramework.delete().forPath(znodePath);
        LOG.info( "delete znode success: " + znodePath );
    }


    /**
     * 删除指定znode 的 children
     * @param znodePath znodePath
     */
    private void deleteChildren(String znodePath) throws Exception {
        Stat stat = curatorFramework.checkExists().forPath(znodePath);
        // stat == null 表示 没有相应 znode
        if ( null == stat) {
            return;
        }
        try {
            interProcessMutex.acquire();
            for(String children :curatorFramework.getChildren().forPath(znodePath)){
                recursionDelete(znodePath + "/" + children);
            }
            LOG.info( "delete znode success: " + znodePath );
        } finally {
            interProcessMutexRelease();
        }
    }


    @Override
    public void close() throws Exception {
        super.close();
        interProcessMutexRelease();
        curatorFramework.close();
    }


    private void initRangePartitionColumnName() {
        List<Integer> rangePartitionColumns = this.table.getPartitionSchema().getRangeSchema().getColumns();
        Validate.isTrue(rangePartitionColumns.size() == 1,
                tableName + " kudu表range 分区对应的列编号为" + rangePartitionColumns + "，只允许有一个 range 分区字段");
        this.rangePartitionIndex = rangePartitionColumns.get(0);
        this.rangePartitionColumnName = fieldsNames[rangePartitionIndex];
    }


    void initCuratorFrameworkAndInterProcessMutex() {
        // 初始化 CuratorFramework 、 InterProcessMutex（互斥锁）
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        curatorFramework = CuratorFrameworkFactory.builder()
                .connectString(commonConf.getDynamicKuduZkUrl())
                .retryPolicy(retryPolicy)
                .sessionTimeoutMs(ZK_SESSION_TIMEOUT_MS)
                .build();
        curatorFramework.start();
        interProcessMutex = new InterProcessMutex(curatorFramework, rootZnode + "/lock");
    }


    /**
     * checkAndCreate root 相关 znode
     * @throws Exception
     */
    private void checkAndCreateBaseRootZnode() throws Exception {
        checkAndCreateZnode(hourRootZnode);
        checkAndCreateZnode(subtaskRootZnode);
        deleteChildren(checkpointRootZnode);
        checkAndCreateZnode(checkpointRootZnode);
    }


    private void checkAndCreateZnode(String znodePath) throws Exception {
        Stat stat = curatorFramework.checkExists().forPath(znodePath);
        // stat == null 表示 没有相应 znode
        if ( null == stat) {
            try {
                interProcessMutex.acquire();
                Thread.sleep(2);
                if(null == curatorFramework.checkExists().forPath(znodePath)){
                    LOG.warn(getRuntimeContext().getIndexOfThisSubtask() + " create znode   " + znodePath);
                    curatorFramework.create().creatingParentsIfNeeded().forPath(znodePath);
                }
            } finally {
                interProcessMutexRelease();
            }
        }
    }


    /**
     * 检查并且创建缺失的 znode,删除多余 znode
     */
    private void checkAndCreateMissZnode() throws Exception {
        List<Long> existZnode = getHourZnodeRecent24Hour();
        // 小时粒度的 znode 数量不足
        if( existZnode.size() < ZNODE_TTL ) {
            try {
                interProcessMutex.acquire();
                // 获取到锁之后，必须重新获取 existZnode，类似于单例模式的双重检测锁
                existZnode = getHourZnodeRecent24Hour();
                if ( existZnode.size() < ZNODE_TTL ) {
                    List<Long> missHours = getMissHours(existZnode);
                    createMissZnode(missHours);
                }
            } catch (Exception e) {
                LOG.error("checkAndCreateMissZnode failed.", e);
            } finally {
                interProcessMutexRelease();
            }
        }

        // Leader 节点删掉那些多余的 znode
        if( KuduSinkStatus.Leader.equals(kuduSinkStatus) ){
            Iterator<Long> iterator = currentPartitionState.get().iterator();
            if (iterator.hasNext()){
                long currentPartition = iterator.next();
                LOG.warn("checkpoint 中保存的 currentPartition 为：{}", currentPartition);
                long currentHour = currentPartition / 100;
                String currentHourZnodePath = hourRootZnode + "/" + currentHour;
                Stat stat = curatorFramework.checkExists().forPath(currentHourZnodePath);
                // stat == null 表示 没有相应 znode
                if ( null == stat) {
                    LOG.warn("znode ： {} 不存在", currentHourZnodePath);
                } else {
                    long[] partitions = curatorFramework.getChildren()
                            .forPath(currentHourZnodePath)
                            .stream()
                            .mapToLong(NumberUtils::toLong)
                            .filter(partition -> partition > currentPartition)
                            .toArray();
                    for(long partition: partitions){
                        recursionDelete(currentHourZnodePath + "/" + partition );
                        LOG.warn("delete znode: " + currentHourZnodePath + "/" + partition);
                        dropRangePartition(rangePartitionColumnName, partition);
                        LOG.warn( rangePartitionColumnName + "    dropRangePartition : " + partition);
                    }
                }
            }
        }
    }


    /**
     *
     * @return 返回最近 24 小时已经存在的 znode
     */
    List<Long> getHourZnodeRecent24Hour(){
        try {
            List<String> hours = curatorFramework.getChildren().forPath(hourRootZnode);
            long hourBefore24 = getBeforeHour(ZNODE_TTL, TimeUnit.HOURS);
            return hours.stream()
                    .map(NumberUtils::toLong)
                    .filter(hour -> hour.compareTo(hourBefore24) > 0
                            && hour.compareTo(getCurrentHour()) <= 0)
                    .collect(Collectors.toList());
        } catch (Exception e) {
            LOG.error(getRuntimeContext().getIndexOfThisSubtask() + " fail.", e);
        }
        return Lists.newArrayList();
    }



    /**
     *
     * @return 返回 24 小时之前存在的 znode
     */
    List<Long> getHourZnodeBefore24Hour(){
        try {
            List<String> hours = curatorFramework.getChildren().forPath(hourRootZnode);
            long hourBefore24 = getBeforeHour(ZNODE_TTL, TimeUnit.HOURS);
            return hours.stream()
                    .map(NumberUtils::toLong)
                    .filter(hour -> hour.compareTo(hourBefore24) < 0)
                    .collect(Collectors.toList());
        } catch (Exception e) {
            LOG.error(getRuntimeContext().getIndexOfThisSubtask() + " fail.", e);
        }
        return Lists.newArrayList();
    }


    /**
     *
     * @param existZnode 已经存在的 znode
     * @return 根据已经存在的 Znode，返回缺失的 znode
     */
    private List<Long> getMissHours(List<Long> existZnode){
        List<Long> targetHours = Lists.newArrayList();
        for( int i = 0; i < ZNODE_TTL; i++ ) {
            long hour = getBeforeHour( i, TimeUnit.HOURS);
            targetHours.add(hour);
        }
        return targetHours
                .stream()
                .filter(t-> !existZnode.contains(t))
                .collect(Collectors.toList());
    }


    /**
     *
     * @param missHours 缺失 znode 的小时分区
     */
    private void createMissZnode(List<Long> missHours) {
        for(long missHour: missHours) {
            initHourZnode(missHour, false);
        }
    }


    /**
     *
     * @param hour 初始化 hour 对应的 znode，包括 hour 以及 partition 对应的 znode
     */
    private void initHourZnode(long hour, boolean isCreateNewPartition) {
        try {
            // 直接创建小时 以及 partition 对应的 znode
            String hourZnode  = this.hourRootZnode + "/" + hour;
            Collection<CuratorTransactionResult> commit = curatorFramework.inTransaction()
                    .create().withMode(CreateMode.PERSISTENT).forPath(hourZnode, Boolean.toString(isCreateNewPartition).getBytes())
                    .and()
                    .create().withMode(CreateMode.PERSISTENT).forPath(hourZnode + "/" + hour + "00", Longs.toByteArray(0))
                    .and().commit();

            for(CuratorTransactionResult curatorTransactionResult: commit){
                LOG.info("znode: {} {} 成功",curatorTransactionResult.getForPath(),curatorTransactionResult.getType());
            }
        } catch ( Exception e) {
            LOG.error("initHourZnode fail.", e);
        }
    }


    /**
     * 等待所有的 SubTask 去 zk 注册完成
     * @throws Exception zk
     */
    private void registerAndAwaitAllSubtaskAlive() throws Exception {
        registerZnode(subtaskRootZnode);
        awaitAllSubtaskRegister(subtaskRootZnode, TimeUnit.SECONDS, 60);
    }

    /**
     * subtask 在指定的znode 下创建 znode 来注册，znode 名为 IndexOfThisSubtask
     * @param registerPath 注册路径
     * @throws Exception zk
     */
    private void registerZnode(String registerPath) throws Exception {
        String forPath = curatorFramework.create()
                .creatingParentsIfNeeded()
                .withMode(CreateMode.EPHEMERAL)
                .forPath(registerPath + "/" + getRuntimeContext().getIndexOfThisSubtask());

        LOG.info("临时 znode {} 注册成功", forPath);
    }


    /**
     * watch 指定 znode 的 children， children 数量 = NumberOfParallelSubtasks 表示注册完成
     * @param registerPath 注册路径
     * @throws Exception zk
     */
    private void awaitAllSubtaskRegister(String registerPath, TimeUnit timeUnit, long timeout) throws Exception {
        long startTs = System.currentTimeMillis();
        long timeoutMs = timeUnit.toMillis(timeout);

        PathChildrenCache hourNodeCache = new PathChildrenCache(curatorFramework, registerPath, false);
        hourNodeCache.start();
        AtomicInteger childrenNum = new AtomicInteger(0);
        hourNodeCache.getListenable().addListener((zkClient, event) -> {
            LOG.info("awaitAllSubtaskRegister, eventType: {}  eventData:{}", event.getType(), event.getData());
            childrenNum.set(hourNodeCache.getCurrentData().size());
        });

        childrenNum.set(hourNodeCache.getCurrentData().size());

        while(childrenNum.get() < getRuntimeContext().getNumberOfParallelSubtasks()) {
            Thread.sleep(5);
            if(System.currentTimeMillis() - startTs > timeoutMs) {
                throw new RuntimeException("awaitAllSubtaskRegister timeout, except children is " + getRuntimeContext().getNumberOfParallelSubtasks()
                            + ", current register subtask number is " + childrenNum.get());
            }
        }
        LOG.info("znode: {} awaitAllSubtaskRegister complete, current register subtask number is {}",registerPath, childrenNum.get());
        hourNodeCache.close();
    }


    /**
     * 从 znode 中
     * 1. 恢复 currentHour
     * 2. 恢复每个小时目前正在往哪个 partition 写入数据，并且写入多少数据了
     */
    private void restoreCurrentHourAndHourPartitionMap() {

        hourPartitionMap = Maps.newHashMap();

        try {
            // 获取当前有哪些小时的数据，且过滤掉 24 之前的小时数据，将分区转换为 Long 类型

            List<String> hours = curatorFramework.getChildren().forPath(hourRootZnode);
            long hourBefore24 = getBeforeHour(ZNODE_TTL, TimeUnit.HOURS);
            List<Long> validHours =  hours.stream()
                    .map(NumberUtils::toLong)
                    .filter(hour -> hour.compareTo(hourBefore24) > 0
                            && hour.compareTo(getCurrentHour()) <= 0)
                    .collect(Collectors.toList());

            validHours.sort(Comparator.reverseOrder());
            // 获取 zk 中保存的最大时间作为真正的 currentHour
            currentHour.add(validHours.get(0));
            for(Long hour : validHours) {
                long hourPartition = getHourLatestPartition(hourRootZnode + "/" + hour);
                // todo 测试阶段先设置为true ，生产环境必须为 false
                hourPartitionMap.put(hour, MutablePair.of(false, hourPartition));
            }
        } catch (Exception e) {
            LOG.error("restore CurrentHour And HourPartitionMap fail.", e);
        }
    }


    /**
     * 获取指定小时 znode 的最后一个 partition 编号
     * @param hourPath hourPath
     * @return 指定hour 对应的 LatestPartition
     */
    private long getHourLatestPartition(String hourPath) throws Exception {

        return curatorFramework.getChildren()
                .forPath(hourPath)
                .stream()
                .mapToLong(NumberUtils::toLong)
                .max().getAsLong();
    }


    /**
     * getCurrentHourPartitionPath
     * @return currentHourPartitionPath
     */
    private String getCurrentHourPartitionPath(){
        return hourRootZnode + "/" + currentHour.getLocalValue() + "/"
                + hourPartitionMap.get(currentHour.getLocalValue()).right;
    }


    /**
     *
     * @return 当前时间戳对应的小时分区
     */
    private long getCurrentHour() {
        return NumberUtils.toLong(new DateTime(System.currentTimeMillis()).toString(DATE_FORMAT));
    }


    /**
     *
     * @param beforeTime 多长时间之前
     * @param timeUnit 时间单位
     * @return 指定时间之前对应的小时分区
     */
    private long getBeforeHour(long beforeTime, TimeUnit timeUnit) {
        return NumberUtils.toLong(new DateTime(System.currentTimeMillis() - timeUnit.toMillis(beforeTime)).toString(DATE_FORMAT));
    }


    /**
     *
     * @return 当前小时是否需要创建新的 partition
     */
    private boolean isCreateNewPartition(){
        MutablePair<Boolean, Long> hourPartitionState = hourPartitionMap.get(currentHour.getLocalValue());

        /*
        当前小时的 创建 Partition 标志为 true
        当前小时目前创建的分区数小于 99
        当前分区写入的数据量 大于设定阈值 || 当前分区写入的数据时长 大于设定阈值
         */
        return  hourPartitionState.left
                && hourPartitionState.right < currentHour.getLocalValue() * 100 + 99
                && (currentPartitionCounter.getLocalValue() > dynamicPartitionMaxRecord
                || System.currentTimeMillis() - currentPartitionTimestamp.getLocalValue() > dynamicPartitionTimeout);
    }


    /**
     * 创建新的 partition 和 相应znode
     */
    private void createRangePartitionAndZnode(String registerPath) throws Exception {
        // 创建 range 分区
        long currentHourPartition = hourPartitionMap.get(currentHour.getLocalValue()).right;
        long targetHourPartition = currentHourPartition + 1;
        createRangePartition(rangePartitionColumnName, targetHourPartition);

        // range 分区如果建好了，就创建 相应 znode
        // 如果 range 分区 或 znode 创建失败，则 这一批次并不切换新分区
        if (isExistsRangePartition(targetHourPartition)) {
            String partitionZnodePath = hourRootZnode + "/" + currentHour.getLocalValue() + "/" + targetHourPartition;
            curatorFramework.create().forPath(partitionZnodePath, Longs.toByteArray(0));
            curatorFramework.setData().forPath(registerPath, CHECKPOINT_LEADER_STATUS.CREATE_NEW_PARTITION.getZkData());
            currentPartitionState.clear();
            currentPartitionState.add(targetHourPartition);
            LOG.info("currentPartitionState 中保存的状态为 {} ",targetHourPartition);
        } else {
            curatorFramework.setData().forPath(registerPath, CHECKPOINT_LEADER_STATUS.NOT_CREATE_NEW_PARTITION.getZkData());
        }
    }



    /**
     * 该方法只负责创建 RangePartition，分布式锁的问题由调用者考虑
     * @param columnName rangePartitionColumnName
     * @param val 新创建的 range 分区的值
     */
    private void createRangePartition(String columnName, long val) {
        // 不存在指定分区，则创建指定分区
        if (isNotExistsRangePartition(val)) {
            PartialRow lower = this.table.getSchema().newPartialRow();
            PartialRow upper = this.table.getSchema().newPartialRow();
            lower.addLong(columnName, val);
            upper.addLong(columnName, val + 1);
            try {
                client.alterTable(tableName, new AlterTableOptions().addRangePartition(lower, upper));
                LOG.info("{} 创建分区 {}  成功", tableName, val);
                Thread.sleep(30);
            } catch (KuduException | InterruptedException e) {
                LOG.error("{} 创建分区失败, msg:{}", tableName, e.getMessage());
            }
        }
    }



    /**
     * 该方法只负责删除 RangePartition，分布式锁的问题由调用者考虑
     * @param columnName dropRangePartition
     * @param val 删除的 range 分区的值
     */
    private void dropRangePartition(String columnName, long val) {
        // 存在指定分区，则删除指定分区
        if (isExistsRangePartition(val)) {
            PartialRow lower = this.table.getSchema().newPartialRow();
            PartialRow upper = this.table.getSchema().newPartialRow();
            lower.addLong(columnName, val);
            upper.addLong(columnName, val + 1);
            try {
                client.alterTable(tableName, new AlterTableOptions().dropRangePartition(lower, upper));
                Thread.sleep(30);
            } catch (KuduException | InterruptedException e) {
                LOG.error("{} 删除分区失败, msg:{}", tableName, e.getMessage());
            }
        }
    }


    /**
     * 判断是否存在指定分区
     * @param val 分区
     * @return 如果不存在返回 true，反之返回 false
     */
    private boolean isNotExistsRangePartition(long val) {
        return !isExistsRangePartition(val);
    }


    /**
     * 判断是否存在指定分区
     * @param val 分区
     * @return 如果存在返回 true，反之返回 false
     */
    private boolean isExistsRangePartition(long val){
        try {
            List<String> rangePartitions = table.getFormattedRangePartitions(10000);
            return rangePartitions.contains(convertRangePartition(val));
        } catch (Exception e) {
            LOG.error("判断是否存在 {} 分区失败，msg:{}", val, e.getMessage());
            return false;
        }
    }


    /**
     *
     * @param val 分区
     * @return 转换成 Kudu api 返回的分区格式
     */
    private String convertRangePartition(long val) {
        return "VALUE = " + val;
    }


    /**
     * 释放分布式互斥锁
     */
    private void interProcessMutexRelease(){
        if(interProcessMutex.isAcquiredInThisProcess()){
            try {
                interProcessMutex.release();
            } catch (Exception e) {
                LOG.error("Release interProcessMutex fail", e);
            }
        }
    }
}

