package com.rui.ds.steps.funs

import com.github.jasync.sql.db.Connection
import com.github.jasync.sql.db.QueryResult
import com.github.jasync.sql.db.general.ArrayRowData
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.async.ResultFuture
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction
import org.apache.flink.types.Row
import org.asynchttpclient.AsyncHttpClient
import org.asynchttpclient.Response
import java.util.concurrent.CompletableFuture
import kotlin.math.min

typealias AsyncRowFunction = RichAsyncFunction<Row, Row>

abstract class JdbcAsyncFunction(private val client: AsyncDatabaseClient) : AsyncRowFunction() {
    override fun asyncInvoke(input: Row, resultFuture: ResultFuture<Row>) {
        val outputResult = client.query(input)

        CompletableFuture.supplyAsync {
            outputResult.get()
        }.thenAccept {
            resultFuture.complete(processResult(input, it))
        }
    }

    abstract fun processResult(input: Row, outputRows: List<Row>): List<Row>
}

class HttpAsyncFunction : AsyncRowFunction() {

    override fun open(parameters: Configuration) {

    }

    override fun asyncInvoke(input: Row, resultFuture: ResultFuture<Row>) {

    }
}

abstract class HttpAsyncClient(
    private val httpClient: AsyncHttpClient
) {
    fun query(input: Row): CompletableFuture<Row> {
        val resultFuture = httpClient.preparePost("body").execute()
        val result = CompletableFuture.supplyAsync {
            val resultSet = resultFuture.get()

            toRow(input, resultSet)
        }

        return result
    }

    abstract fun toRow(input: Row, response: Response): Row
}

class AsyncDatabaseClient(
    private val dbClient: Connection,
    private val querySql: String,
    private val params: List<String>,
    private val rowLimit: Int
) {
    fun query(input: Row): CompletableFuture<List<Row>> {
        val resultFuture = dbClient.sendPreparedStatement(querySql, params.map { input.getField(it) })
        val result = CompletableFuture.supplyAsync {
            val resultSet = resultFuture.get()

            toRows(resultSet)
        }

        return result
    }

    private fun toRows(queryResult: QueryResult): List<Row> {
        val rows = queryResult.rows
        if (rows.isEmpty()) {
            return emptyList()
        }

        return rows.map {
            val columnNames = (it as ArrayRowData).mapping
            val output = Row.withNames()
            columnNames.forEach { (name, index) ->
                output.setField(name, it.columns[index])
            }
            output
        }.subList(0, min(rowLimit, rows.size))
    }
}
