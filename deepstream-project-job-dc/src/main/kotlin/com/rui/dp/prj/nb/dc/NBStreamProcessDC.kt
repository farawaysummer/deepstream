package com.rui.dp.prj.nb.dc

import com.google.common.base.Strings
import com.rui.dp.prj.base.AsyncDBJoinFunction
import com.rui.dp.prj.base.BusinessData
import com.rui.dp.prj.base.DeepStreamHelper
import com.rui.dp.prj.base.ProjectJob
import com.rui.ds.ProcessContext
import org.apache.flink.streaming.api.datastream.AsyncDataStream
import java.util.concurrent.TimeUnit

class NBStreamProcessDC: ProjectJob {
    private var context: ProcessContext = DeepStreamHelper.initEnv()
    private val businessData: BusinessData = DeepStreamHelper.loadBusiness()
    private val copyFunction = CopyStreamFunction()

    override fun init() {
        // parse business data
    }

    override fun prepare() {
        for (tableName in businessData.relatedTables) {
            val tableSql = DeepStreamHelper.getSql(tableName)
            if (!Strings.isNullOrEmpty(tableSql)) {
                DeepStreamHelper.executeSQL(context, tableSql!!)
            }
        }
    }

    override fun start() {
        // query from kafka s_cli_recipe
        val queryCliSql = DeepStreamHelper.getSql("queryCLI")
        val cliDataStream = DeepStreamHelper.executeQuery(context, queryCliSql!!)

        // copy stream to outputTag ProcessTag
        val rowDataStream = context.tableEnv.toChangelogStream(cliDataStream)
        val mainStream = rowDataStream.process(copyFunction)

        // main stream insert into s_cli_recipe_cdc
        context.tableEnv.createTemporaryView("DTable", mainStream)

        val cdcInsertSql = DeepStreamHelper.getSql("insertCDC")
        DeepStreamHelper.executeSQL(context, cdcInsertSql!!)

        // ProcessTag stream join data
        val processStream = mainStream.getSideOutput(copyFunction.defaultTag)

        val asyncFunction = AsyncDBJoinFunction(businessData)

        val streamDataType = DeepStreamHelper.toStreamDataTypes(businessData.resultFields)

        val queryResult = AsyncDataStream.unorderedWait(
            processStream,
            asyncFunction,
            30000, TimeUnit.SECONDS
        )
            .returns(
                streamDataType.toTypeInformation()
            )

        // ProcessTag stream insert into c01 kafka table

        context.tableEnv.createTemporaryView("C01Table", queryResult)

        val insertC01Sql = DeepStreamHelper.getSql("insertC01")
        DeepStreamHelper.executeSQL(context, insertC01Sql!!)
    }

    override fun clean() {

    }
}