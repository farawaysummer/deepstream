package com.rui.ds.prj.test

import com.rui.dp.prj.job.DeepStreamHelper
import com.rui.ds.datasource.DatabaseSources
import org.apache.commons.compress.utils.Lists
import org.apache.flink.types.Row

object DBTest {
    init {
        DeepStreamHelper.loadDatasource()
        DeepStreamHelper.loadSql()
    }

    @JvmStatic
    fun main(args: Array<String>) {
        DatabaseSources.getConnection("DIP").use { connection ->
            val sql = DeepStreamHelper.getSql("KTDOC_WORK_LOG")
            val statement = connection!!.prepareStatement(sql)

            println("====Start execute query")
            val result = statement.executeQuery()

            val metaData = result.metaData
            for (index in 1 .. metaData.columnCount) {
                val name  = metaData.getColumnName(index)
                val type = metaData.getColumnTypeName(index)

                println("<field name=\"$name\" type=\"$type\"/>")
            }

        }
    }
}