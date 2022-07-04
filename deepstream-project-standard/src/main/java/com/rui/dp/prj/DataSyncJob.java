package com.rui.dp.prj;

import com.rui.dp.prj.base.*;
import com.rui.ds.ProcessContext;

public class DataSyncJob implements ProjectJob {
    private final ProcessContext context = DeepStreamHelper.initEnv();
    private final DeepStreamSyncJobData jobData;

    public DataSyncJob(DeepStreamSyncJobData jobData) {
        this.jobData = jobData;
    }

    @Override
    public void prepare() {
        // 创建Flink表的定义
        for (RelatedTable tableRef : jobData.getRelatedTables()) {
            String tableSql = tableRef.toTableSql();
            DeepStreamHelper.executeSQL(context, tableSql);
        }
    }

    @Override
    public void start() {

    }

    @Override
    public void clean() {

    }
}
