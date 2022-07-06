package com.rui.dp.prj.base.job

data class DeepStreamSyncJobData(
    val jobName: String,
    val relatedTables: List<RelatedTable>,
    val syncSQLs: List<String>
): java.io.Serializable {
    private val SQLS: MutableMap<String, String> = mutableMapOf()

    internal fun setSQLs(sqls: Map<String, String>) {
        this.SQLS.putAll(sqls)
    }

    fun getSQL(name: String): String {
        return SQLS[name] ?: throw RuntimeException("未找到SQL: $name")
    }

}