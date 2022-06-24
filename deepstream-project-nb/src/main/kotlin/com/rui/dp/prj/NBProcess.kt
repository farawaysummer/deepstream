package com.rui.dp.prj

import com.google.common.base.Strings
import com.google.common.collect.Lists
import com.rui.dp.prj.job.DeepStreamHelper.executeQuery
import com.rui.dp.prj.job.DeepStreamHelper.executeSQL
import com.rui.dp.prj.job.DeepStreamHelper.getSql
import com.rui.dp.prj.job.DeepStreamHelper.initEnv
import com.rui.ds.ProcessContext
import com.rui.ds.StreamDataTypes
import org.apache.flink.table.catalog.ResolvedSchema

class NBProcess {
    private var context: ProcessContext? = null
    private var processFunction: NBProcessFunction? = null

    fun init() {
        context = initEnv()
        processFunction = NBProcessFunction()
    }

    fun createTables() {
        // create table
        for (tableName in tables) {
            val tableSql = getSql(tableName)
            if (!Strings.isNullOrEmpty(tableSql)) {
                executeSQL(context!!, tableSql!!)
            }
        }
    }

    fun startProcess() {
        val mainSql = getSql("queryCF")
        val mainResult = executeQuery(context!!, mainSql!!)

        context!!.tableEnv.createTemporaryView("DTable", mainResult)

        val insertSql = getSql("insertCF")

        executeSQL(context!!, insertSql!!)
    }

    fun startProcessTest() {
        val mainSql = getSql("queryTest")
        executeSQL(context!!, mainSql!!).print()

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