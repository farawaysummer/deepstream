package com.rui.ds.facade.kettle.debug

import com.rui.ds.common.TableContext
import com.rui.ds.common.TableContext.Companion.TABLE_TYPE_DIM
import com.rui.ds.common.TableContext.Companion.TABLE_TYPE_SINK
import org.junit.Test
import kotlin.test.assertNotNull

class DebugCase : DeepStreamDebugger() {

    @Test
    fun `debug test case1`() {
        val context = initEnv()

        assertNotNull(context)

        createTable(context, "eigdb", TableContext("eigdb", "R_CLI_FEE_DETAIL", TABLE_TYPE_DIM))
        createTable(context, "eigdb", TableContext("eigdb", "R_MPI_PATIENTINFO", TABLE_TYPE_DIM))

        val table = executeQuery(context, SQL["selectFromFeeDetail"]!!)
        println("===================${dataByTable(table.resolvedSchema)}")

        // 可以得知输入的表的列定义
        context.tableEnv.createTemporaryView("DTable", table)
//        context.tableEnv.createTemporarySystemFunction("to_str", StringFunction())

        // 创建的表，是否能否实时获取定义

        createTable(context, "eiginterface", TableContext("", "DI_ADI_EXPSET_LIST_NEU", TABLE_TYPE_SINK))

        val listTable = context.tableEnv.from("DI_ADI_EXPSET_LIST_NEU")
        val targetType = dataByTable(
            listTable.resolvedSchema
        )
        println("===================$targetType)}")
        executeSQL(
            context, SQL["insertIntoInterface"]!!
        ).print()
    }

}