package com.rui.dp.prj;

import com.rui.dp.prj.base.*;
import com.rui.dp.prj.base.delay.DelayProcessService;
import com.rui.dp.prj.base.funs.AsyncDBJoinFunction;
import com.rui.dp.prj.base.funs.DeduplicateRowFunction;
import com.rui.dp.prj.base.funs.ResultForwardFunction;
import com.rui.dp.prj.base.funs.ValueMappingFunction;
import com.rui.dp.prj.base.job.*;
import com.rui.ds.ProcessContext;
import com.rui.ds.StreamDataTypes;
import com.rui.ds.ks.delay.DelayServiceKey;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class DataProcessJob implements ProjectJob {
    private static final Logger logger = LoggerFactory.getLogger(DataProcessJob.class);

    private final ProcessContext context;
    private final ProcessJobData jobData;

    public DataProcessJob(ProcessJobData jobData) {
        this.jobData = jobData;
        context = DeepStreamHelper.initEnv(jobData.getJobName());
    }

    @Override
    public void prepare() {

        logger.info("Ready to prepare job [" + jobData.getJobName() + "]");
        // 创建Flink表的定义
        for (RelatedTable tableRef : jobData.getRelatedTables()) {
            String tableSql = tableRef.toTableSql();
            logger.info("[" + jobData.getJobName() + "] Execute related table create sql:\n" + tableSql);
            DeepStreamHelper.executeSQL(context, tableSql);
        }

        // 创建事件读取的表定义, 初始化事件资源
        List<EventData> events = jobData.getEvents();
        for (EventData eventData : events) {
            String eventSql = eventData.toEventTableSql();
            logger.info("[" + jobData.getJobName() + "] Execute event table create sql:\n" + eventSql);
            DeepStreamHelper.executeSQL(context, eventSql);

            if (eventData.getDelayRetryConfig().getEnabled()) {
                DelayServiceKey key = new DelayServiceKey(eventData.getDelayRetryConfig().getServers(),
                        eventData.getDelayRetryConfig().getLevel());
                logger.info("[" + jobData.getJobName() + "] Register delay process {}", key);

                DelayProcessService.registerDelayProcess(key);
            }
        }

        logger.info("Finish preparing job [" + jobData.getJobName() + "]");

        jobData.loadDataSource();
    }

    @Override
    public void start() {
        logger.info("Create processing job [" + jobData.getJobName() + "]");
        List<EventData> events = jobData.getEvents();
        List<DataStream<Row>> allQueryResults = Lists.newArrayList();

        // 是否需要值域映射
        ValueMappingFunction mappingFunction = null;
        if (jobData.getProcessData().useDictMapping()) {
            List<String> columns = jobData.getProcessData().getResultFields()
                    .stream().map(DataField::getFieldName).collect(Collectors.toList());
            mappingFunction = DeepStreamFunctions.createValueMappingFunctions(
                    jobData,
                    jobData.getProcessData().getDictTransforms(),
                    columns
            );
        }

        for (EventData event : events) {
            // 对数据变更事件去重
            Table eventTable = DeepStreamHelper.executeQuery(context, event.toEventQuerySql());
            DataStream<Row> eventStream = context.getTableEnv().toChangelogStream(eventTable)
                    .assignTimestampsAndWatermarks(
                            WatermarkStrategy.<Row>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                                    .withTimestampAssigner((SerializableTimestampAssigner<Row>)
                                            (element, l) -> {
                                                Instant timestamp = (Instant) element.getField(Consts.FIELD_PROC_TIME);
                                                if (timestamp == null) {
                                                    return System.currentTimeMillis();
                                                } else {
                                                    return timestamp.toEpochMilli();
                                                }
                                            }));

            List<String> keyFields = Lists.newArrayList();
            event.getEventFields().forEach(
                    it -> {
                        keyFields.add(it.getFieldName());
                    }
            );

            // 按事件分组
            StreamDataTypes eventTypes = DeepStreamHelper.toStreamDataTypes(event.getEventFields());
            DataStream<Row> groupStream = eventStream.keyBy(row -> RowDesc.of(row, keyFields))
                    .process(new DeduplicateRowFunction(jobData.getJobName(), event))
                    .returns(eventTypes.toTypeInformation());

            QueryData queryData = jobData.createQueryData(event);

            // 关联查询
            StreamDataTypes streamDataType = DeepStreamHelper.toStreamDataTypes(jobData.getProcessData().getResultFields());
            DataStream<Row> queryResult = AsyncDataStream.unorderedWait(
                            groupStream,
                            new AsyncDBJoinFunction(queryData),
                            30000, TimeUnit.SECONDS
                    ).process(new ResultForwardFunction(queryData))
                    .returns(streamDataType.toTypeInformation());

            if (mappingFunction != null) {
                queryResult = queryResult
                        .map(mappingFunction)
                        .returns(streamDataType.toTypeInformation());
            }

            allQueryResults.add(queryResult);
        }

        DataStream<Row> resultStream;
        if (allQueryResults.size() == 1) {
            resultStream = allQueryResults.get(0);
        } else {
            DataStream<Row> first = allQueryResults.get(0);
            DataStream<Row>[] rest = allQueryResults.subList(1, allQueryResults.size()).toArray(new DataStream[0]);
            resultStream = first.union(rest);
        }

        // 处理完成后，插入目标表
        context.getTableEnv().createTemporaryView(Consts.RESULT_TABLE, resultStream);
        String insertSql = jobData.getSQL(jobData.getProcessData().getSinkSqlName());
        DeepStreamHelper.executeSQL(context, insertSql);

        logger.info("[" + jobData.getJobName() + "] Start to process");
    }

    @Override
    public void clean() {

    }
}
