package com.rui.dp.prj

import com.google.common.base.Strings
import com.rui.dp.prj.job.DeepStreamHelper
import com.rui.dp.prj.job.DeepStreamHelper.executeQuery
import com.rui.dp.prj.job.DeepStreamHelper.getSql
import com.rui.ds.ProcessContext
import org.apache.flink.streaming.api.datastream.AsyncDataStream
import java.util.concurrent.TimeUnit

class NBStreamProcess {
    private var context: ProcessContext = DeepStreamHelper.initEnv()

    init {
    }

    fun createTables() {
        // create table
        for (tableName in tables) {
            val tableSql = getSql(tableName)
            if (!Strings.isNullOrEmpty(tableSql)) {
                DeepStreamHelper.executeSQL(context, tableSql!!)
            }
        }
    }

    fun startProcess() {
        val mainSql = getSql("querySingle")
        val mainResult = executeQuery(context, mainSql!!)

        val rowDataStream = context.tableEnv.toChangelogStream(mainResult)

        val asyncFunction = SampleAsyncFunction()

        val queryResult = AsyncDataStream.unorderedWait(rowDataStream, asyncFunction, 30000, TimeUnit.SECONDS)

        context.tableEnv.createTemporaryView("DTable", queryResult)

        DeepStreamHelper.executeSQL(context, "select * from DTable").print()
//        val insertSql = getSql("insert")
//
//
//        context.tableEnv.createTemporaryView("DTable", outputTable)
//
//        executeSQL(context, insertSql!!)

    }

    companion object {
        private val tables = listOf(
            "KSMC",
            "ZGXX",
            "MZGHXX",
            "SYSVAR",
            "CFFL",

            "YZYFB",
            "GYFSB",
            "YPJX",
            "MZCFMXXX",
            "YPMCQT",

            "MZCFXX",
            "YPMC",
            "MZSFXX",
            "KTDOC_DIAGNOSE",

            "KTDOC_CASE_HISTORY",
            "KTDOC_WORK_LOG",
            "KTDOC_PRESCRIPTION",
            "KTDOC_PRESCRIPTION_LIST",

            "S_CLI_RECIPE"
        )
    }
}