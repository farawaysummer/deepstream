package com.rui.dp.prj.nb.cf

import com.google.common.base.Strings
import com.rui.dp.prj.base.AsyncDBJoinFunction
import com.rui.dp.prj.base.BusinessData
import com.rui.dp.prj.base.DeepStreamHelper
import com.rui.dp.prj.base.DeepStreamHelper.executeQuery
import com.rui.dp.prj.base.DeepStreamHelper.executeSQL
import com.rui.dp.prj.base.DeepStreamHelper.getSql
import com.rui.dp.prj.base.DeepStreamHelper.toStreamDataTypes
import com.rui.dp.prj.base.ProjectJob
import com.rui.ds.ProcessContext
import org.apache.flink.streaming.api.datastream.AsyncDataStream
import java.util.concurrent.TimeUnit

class NBStreamProcessCF : ProjectJob {
    private var context: ProcessContext = DeepStreamHelper.initEnv()
    private val businessData: BusinessData = DeepStreamHelper.loadBusiness()

    override fun init() {
        // parse business data
    }

    override fun prepare() {
        for (tableName in businessData.relatedTables) {
            val tableSql = getSql(tableName)
            if (!Strings.isNullOrEmpty(tableSql)) {
                executeSQL(context, tableSql!!)
            }
        }
    }

    override fun start() {
        val mainSql = getSql("querySingle")
        val mainResult = executeQuery(context, mainSql!!)

        val rowDataStream = context.tableEnv.toChangelogStream(mainResult)

        val asyncFunction = AsyncDBJoinFunction(businessData)

        val streamDataType = toStreamDataTypes(businessData.resultFields)

        val queryResult = AsyncDataStream.unorderedWait(
            rowDataStream,
            asyncFunction,
            30000, TimeUnit.SECONDS
        )
            .returns(
                streamDataType.toTypeInformation()
            )

        context.tableEnv.createTemporaryView("DTable", queryResult)

        val insertSql = getSql("insert")
        executeSQL(context, insertSql!!)
    }

    override fun clean() {

    }

}