package com.rui.ds.steps.app

import com.rui.ds.ProcessContext
import com.rui.ds.common.DataContext
import com.rui.ds.common.TransformStep

class PrintStep(name:String, meta: DefaultMeta): TransformStep(name, meta) {
    override fun process(data: DataContext, process: ProcessContext): DataContext {
        val table = toTable(data, process)
        process.tableEnv.createTemporaryView("NTable", table)

        process.tableEnv.executeSql("select * from NTable").print()

        return data
    }
}

