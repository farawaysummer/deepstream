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
            val timestamp = (row.getField(Consts.FIELD_PROC_TIME) as Instant).toEpochMilli()
            val rowKind =
                if (row.kind == RowKind.DELETE) {
                    row.kind
                } else {
                    RowKind.INSERT
                }
            val rowKeys = keys.map { row.getField(it) ?: "" }.toList()

            return RowDesc(rowKind, timestamp, rowKeys)
        }
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as RowDesc

        if (rowKind != other.rowKind) return false
        if (rowKeys != other.rowKeys) return false

        return true
    }

    override fun hashCode(): Int {
        var result = rowKind.hashCode()
        result = 31 * result + rowKeys.hashCode()
        return result
    }


}