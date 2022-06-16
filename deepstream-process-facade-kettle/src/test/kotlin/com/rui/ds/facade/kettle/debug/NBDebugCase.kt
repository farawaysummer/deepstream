package com.rui.ds.facade.kettle.debug

import com.rui.ds.common.TableContext
import org.junit.Test
import kotlin.test.assertNotNull

class NBDebugCase : DeepStreamDebugger() {
    @Test
    fun `debug select from nb his`() {
        val context = initEnv()

        assertNotNull(context)

        createTable(context, "DIP", TableContext("DIP", "KTDOC_PRESCRIPTION_LIST", TableContext.TABLE_TYPE_DIM))
        createTable(context, "DIP", TableContext("DIP", "KTDOC_PRESCRIPTION", TableContext.TABLE_TYPE_DIM))
        createTable(context, "DIP", TableContext("DIP", "YPMC", TableContext.TABLE_TYPE_DIM))
        createTable(context, "DIP", TableContext("DIP", "KTDOC_WORK_LOG", TableContext.TABLE_TYPE_DIM))
        createTable(context, "DIP", TableContext("DIP", "KTHRAEVT_CLINIC", TableContext.TABLE_TYPE_DIM))

        executeSQL(context, SQL["select_from_nb_his"]!!).print()


//        // 可以得知输入的表的列定义
//        context.tableEnv.createTemporaryView("DTable", table)
//        context.tableEnv.createTemporarySystemFunction("to_str", StringFunction())
//
//        // 创建的表，是否能否实时获取定义
//
//        createTable(context, "eiginterface", TableContext("", "DI_ADI_EXPSET_LIST_NEU", TableContext.TABLE_TYPE_SINK))
//
//        val listTable = context.tableEnv.from("DI_ADI_EXPSET_LIST_NEU")
//        val targetType = dataByTable(
//            listTable.resolvedSchema
//        )
//        println("===================$targetType)}")
//        executeSQL(
//            context, SQL["insertIntoInterface"]!!
//        ).print()
    }
}