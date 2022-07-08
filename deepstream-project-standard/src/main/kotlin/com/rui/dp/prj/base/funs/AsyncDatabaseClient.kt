package com.rui.dp.prj.base.funs

import com.rui.dp.prj.base.job.QueryData
import com.rui.ds.datasource.DatabaseSources
import org.apache.flink.metrics.Counter
import org.apache.flink.types.Row
import org.apache.flink.types.RowKind
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.math.BigDecimal
import java.sql.SQLException
import java.util.concurrent.CompletableFuture

class AsyncDatabaseClient(private val queryData: QueryData,
                          private val delayQueryTime: Int,
                          private val counter: Counter) {
    init {
        queryData.loadDataSource()
    }

    fun query(key: Row): CompletableFuture<Collection<Row>> {
        return CompletableFuture.supplyAsync { queryDB(key) }
    }

    private fun queryDB(row: Row): List<Row> {
        if (row.kind == RowKind.UPDATE_BEFORE) {
            return emptyList()
        }

        try {
            if (delayQueryTime > 0) { // delay query, in case required data not exist
                Thread.sleep(delayQueryTime * 1000L)
            }

            DatabaseSources.getConnection(queryData.dsName).use { connection ->
                val rowFields = row.getFieldNames(true)

                val sql = if (queryData.dynamicCondition) {
                    val conditionStr = queryData.conditionFields.joinToString(" AND ") { "${queryData.masterTable}.$it = ?" }
                    val businessSql = queryData.businessSql
                    if (businessSql.lastIndexOf(string = "where", ignoreCase = true) >
                        businessSql.lastIndexOf(string = "from", ignoreCase = true)
                    ) {
                        "$businessSql AND $conditionStr"
                    } else {
                        "$businessSql WHERE $conditionStr"
                    }
                } else {
                    queryData.businessSql
                }

                logger.debug("[${queryData.jobName}] Query with sql:\n $sql")

                val rows = mutableListOf<Row>()
                val statement = connection!!.prepareStatement(sql)
                statement.use {
                    for (index in 1..queryData.conditionFields.size) {
                        val eventField = queryData.fieldMapping[queryData.conditionFields[index - 1]]
                            ?: queryData.conditionFields[index - 1]

                        val fieldIndex = rowFields?.indexOf(eventField)
                        if (fieldIndex == null) {
                            statement.setObject(index, row.getField(index - 1))
                        } else {
                            statement.setObject(index, row.getField(fieldIndex))
                        }
                    }

                    val result = statement.executeQuery()
                    while (result.next()) {
                        val values =
                            queryData.resultFields.keys.associateBy({
                                it
                            }, {
                                result.getObject(it)
                            })
                                .mapValues { (_, value) ->
                                    typeNormalize(value)
                                }
                        val newRow = Row.withNames()
                        queryData.resultFields.keys.forEach { newRow.setField(it, values[it]) }

                        rows.add(newRow)
                        counter.inc()
                    }
                    logger.info("[${queryData.jobName}] Process data $row , and finished with ${rows.size} values")

                    return rows
                }

            }
        } catch (e: SQLException) {
            logger.error("Process data $row failed.", e)
            throw RuntimeException(e)
        }
    }

    private fun typeNormalize(value: Any?): Any? {
        if (value == null) {
            return null
        }

        return when (value) {
            is BigDecimal -> value.toString()
            is java.sql.Timestamp -> value.toLocalDateTime()
            is java.sql.Date -> value.toLocalDate()
            is java.sql.Time -> value.toLocalTime()
            else -> value
        }
    }

    companion object {
        val logger: Logger = LoggerFactory.getLogger(AsyncDatabaseClient::class.java)
    }
}