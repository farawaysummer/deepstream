package com.rui.dp.prj.base

import com.rui.ds.datasource.DatabaseSources
import org.apache.flink.types.Row
import org.apache.flink.types.RowKind
import java.math.BigDecimal
import java.sql.SQLException
import java.util.concurrent.CompletableFuture

class AsyncDatabaseClient(private val business: BusinessData) {
    init {
        DeepStreamHelper.loadDatasource()
        DeepStreamHelper.loadSql()
    }

    fun query(key: Row): CompletableFuture<Collection<Row?>> {
        return CompletableFuture.supplyAsync { queryDB(key) }
    }

    private fun queryDB(row: Row): List<Row?> {
        if (row.kind == RowKind.UPDATE_BEFORE) {
            return emptyList()
        }

        try {
            DatabaseSources.getConnection(business.dsName).use { connection ->
                println("Input Row with Name: ${row.getFieldNames(true)}")
                val sql = business.businessSql
                val rows = mutableListOf<Row>()
                val statement = connection!!.prepareStatement(sql)
                for (index in 1..business.conditionFields.size) {
                    statement.setObject(index, row.getField(index - 1))
//                    statement.setObject(index, row.getField(business.conditionFields[index - 1]))
                }

                val result = statement.executeQuery()
                while (result.next()) {
                    val values =
                        business.resultFields.keys.associateBy({ it }, { result.getObject(it) })
                            .mapValues { (_, value) ->
                                if (value is BigDecimal) {
                                    value.toString()
                                } else {
                                    value
                                }
                            }
                    val newRow = Row.withNames()
                    business.resultFields.keys.forEach { newRow.setField(it, values[it]) }

                    rows.add(newRow)
                }
                return rows
            }
        } catch (e: SQLException) {
            e.printStackTrace()
            throw RuntimeException(e)
        }
    }
}