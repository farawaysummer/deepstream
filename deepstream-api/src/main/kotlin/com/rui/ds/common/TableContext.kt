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
        const val TABLE_TYPE_CDC = "cdc"
        const val TABLE_TYPE_DIM = "dim"
        const val TABLE_TYPE_SINK = "sink"
    }
}
