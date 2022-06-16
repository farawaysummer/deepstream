package com.rui.ds.steps.query

import com.rui.ds.ProcessContext
import com.rui.ds.StreamDataTypes
import com.rui.ds.common.DataContext
import com.rui.ds.common.StepMeta
import com.rui.ds.common.TransformStep
import com.rui.ds.datasource.DataBase
import com.rui.ds.datasource.DatabaseSources
import com.rui.ds.steps.funs.AsyncDatabaseClient
import com.rui.ds.steps.funs.JdbcAsyncFunction
import org.apache.flink.streaming.api.datastream.AsyncDataStream
import org.apache.flink.types.Row
import java.util.concurrent.TimeUnit

/**
 * 同TableInput，使用记录中的值作为输入参数
 */
class DatabaseJoinStep(name:String, override val meta: DatabaseJoinStepMeta) : TransformStep(name, meta) {
    private lateinit var addTypes: StreamDataTypes

    private lateinit var asyncDatabaseRequest: AsyncDatabaseJoinFunction

    override fun initStep() {
        addTypes = DataBase.extractDataTypesFromSql(meta.dsName, meta.sql)

        asyncDatabaseRequest = AsyncDatabaseJoinFunction(
            AsyncDatabaseClient(
                DatabaseSources.getAsyncConnection(meta.dsName),
                meta.sql,
                meta.joinFields,
                meta.recordLimit
            )
        )
    }

    override fun process(data: DataContext, process: ProcessContext): DataContext {
        // 根据row的字段组织外部查询
        val stream = toStream(data, process)

        val joinedStream = AsyncDataStream.unorderedWait(
            stream,
            asyncDatabaseRequest,
            1000,
            TimeUnit.MILLISECONDS,
            100
        )

        // 查询结构和原记录合并后返回
        return dataByStream(joinedStream, data.types.plus(addTypes))
    }
}

data class DatabaseJoinStepMeta(
    val sql: String,
    val dsName: String,
    val joinFields: List<String> = emptyList(),
    val recordLimit: Int = 1
) : StepMeta

class AsyncDatabaseJoinFunction(client: AsyncDatabaseClient) : JdbcAsyncFunction(client) {
    override fun processResult(input: Row, outputRows: List<Row>): List<Row> {
        return if (outputRows.isEmpty()) {
            emptyList()
        } else {
            outputRows.map { singleRow ->
                Row.join(input, singleRow)
            }
        }
    }
}