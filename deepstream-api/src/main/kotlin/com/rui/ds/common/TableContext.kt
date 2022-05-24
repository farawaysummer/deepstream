package com.rui.ds.common

data class TableContext(
    val catalog: String,
    val tableName: String,
    val tableType: String
) {
    enum class TableSize {
        SMALL,
        MEDIUM,
        LARGE
    }

    companion object {
        const val TABLE_TYPE_CDC = "cdc"        // CDC表，处理变更记录
        const val TABLE_TYPE_DIM = "dim"        // DIM表，全量表，用于关联事实表数据
        const val TABLE_TYPE_SINK = "sink"      // SINK表，用于处理流式输出
    }
}
