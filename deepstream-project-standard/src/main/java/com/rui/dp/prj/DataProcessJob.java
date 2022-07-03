package com.rui.dp.prj;

import com.rui.dp.prj.base.*;
import com.rui.dp.prj.base.funs.AsyncDBJoinFunction;
import com.rui.dp.prj.base.funs.DeduplicateRowFunction;
import com.rui.dp.prj.base.funs.ValueMappingFunction;
import com.rui.ds.ProcessContext;
import com.rui.ds.StreamDataTypes;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class DataProcessJob implements ProjectJob {
    private final ProcessContext context = DeepStreamHelper.initEnv();
    private final DeepStreamJobData jobData;

    public DataProcessJob(DeepStreamJobData jobData) {
        this.jobData = jobData;
    }

    @Override
    public void prepare() {
        // 创建Flink表的定义
        for (RelatedTable tableRef : jobData.getRelatedTables()) {
            String tableSql = tableRef.toTableSql();
            DeepStreamHelper.executeSQL(context, tableSql);
        }

        // 创建事件读取的表定义
        DeepStreamHelper.executeSQL(context, jobData.getEventData().toEventTableSql());
    }

    @Override
    public void start() {
        List<String> keyFields = Lists.newArrayList();

        // 对数据变更事件去重
        Table eventTable = DeepStreamHelper.executeQuery(context, jobData.getEventData().toEventQuerySql());
        DataStream<Row> eventStream = context.getTableEnv().toChangelogStream(eventTable)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Row>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                                .withTimestampAssigner((SerializableTimestampAssigner<Row>)
                                        (element, l) -> {
                                            Instant timestamp = (Instant) element.getField(Consts.FILE_PROC_TIME);
                                            if (timestamp == null) {
                                                return System.currentTimeMillis();
                                            } else {
                                                return timestamp.toEpochMilli();
                                            }
                                        }));

        // 按事件分组
        StreamDataTypes eventTypes = DeepStreamHelper.toStreamDataTypes(jobData.getEventData().getEventFields());
        DataStream<Row> groupStream = eventStream.keyBy(row -> RowDesc.of(row, keyFields))
                .process(new DeduplicateRowFunction(keyFields))
                .returns(eventTypes.toTypeInformation());

        // 是否需要值域映射
        ValueMappingFunction mappingFunction = null;
        if (jobData.getProcessData().getUseDictMapping()) {
            List<String> columns = jobData.getProcessData().getResultFields()
                    .stream().map(DataField::getFieldName).collect(Collectors.toList());
            mappingFunction = DeepStreamFunctions.createValueMappingFunctions(
                    jobData,
                    jobData.getProcessData().getDictTransforms(),
                    columns
            );
        }

        // 关联查询
        StreamDataTypes streamDataType = DeepStreamHelper.toStreamDataTypes(jobData.getProcessData().getResultFields());
        DataStream<Row> queryResult;
        if (mappingFunction != null) {
            queryResult = AsyncDataStream.unorderedWait(
                    groupStream,
                    new AsyncDBJoinFunction(jobData.getQueryData()),
                    30000, TimeUnit.SECONDS
            ).map(mappingFunction).returns(
                    streamDataType.toTypeInformation()
            );
        } else {
            queryResult = AsyncDataStream.unorderedWait(
                    groupStream,
                    new AsyncDBJoinFunction(jobData.getQueryData()),
                    30000, TimeUnit.SECONDS
            ).returns(
                    streamDataType.toTypeInformation()
            );
        }

        // 处理完成后，插入目标表
        context.getTableEnv().createTemporaryView("DTable", queryResult);
        String insertSql = jobData.getSQL(jobData.getProcessData().getSinkSqlName());
        DeepStreamHelper.executeSQL(context, insertSql);
    }

    @Override
    public void clean() {

    }
}
