package com.rui.dp.prj.base

data class DeepStreamSyncJobData(
    val jobName: String,
    val relatedTables: List<RelatedTable>,
    val syncSQLs: List<String>
)