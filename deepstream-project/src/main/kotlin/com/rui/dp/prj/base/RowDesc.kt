package com.rui.dp.prj.base

import org.apache.flink.types.Row
import org.apache.flink.types.RowKind
import java.time.Instant

data class RowDesc(
    val rowKind: RowKind,
    val timestamp: Long,
    val rowKeys: List<Any>
) {
    companion object {
        @JvmStatic
        fun of(row: Row, keys: List<String>): RowDesc {
            val timestamp = (row.getField("proctime") as Instant).toEpochMilli()
            val rowKind = row.kind
            val rowKeys = keys.map { row.getField(it) ?: "" }.toList()

            return RowDesc(rowKind, timestamp, rowKeys)
        }
    }
}