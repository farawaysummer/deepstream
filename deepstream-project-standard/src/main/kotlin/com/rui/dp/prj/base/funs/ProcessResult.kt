package com.rui.dp.prj.base.funs

import org.apache.flink.types.Row

data class ProcessResult(
    val input: Row,
    val pass: Int,
    val output: Collection<Row>,
    val unfinished: Collection<Row>
) : java.io.Serializable {
    companion object {
        const val ALL_PASS: Int = 1
        const val PART_PASS: Int = 2
        const val ALL_UNFINISHED: Int = 3
    }
}