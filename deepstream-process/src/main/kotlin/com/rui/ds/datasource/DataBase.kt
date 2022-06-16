package com.rui.ds.datasource

import com.rui.ds.StreamDataTypes
import java.sql.Connection
import java.sql.ResultSet
import java.sql.ResultSetMetaData

typealias SQLTypes = java.sql.Types

object DataBase {

    fun extractDataTypesFromSql(dsName: String, sql: String): StreamDataTypes {
        val connection = DatabaseSources.getConnection(dsName) ?: throw RuntimeException("Can't connect.")
        val newSql = prepareSql(sql)

        return if (DatabaseSources.getDataSource(dsName)?.type == "oracle") {
            getQueryFieldsFromDatabaseMetaData(connection, sql)
        } else {
            getQueryFieldsFromPreparedStatement(connection, newSql)
        }
    }

    private fun prepareSql(content: String): String {
        val startMark = "$" + "{"
        val prepareSQL = StringBuilder()
        var index = content.indexOf(startMark)
        var cursor = 0
        while (index != -1) {
            prepareSQL.append(content, cursor, index).append("?")
            cursor = content.indexOf("}", index)
            if (cursor != -1) {
                val name = content.substring(index + 2, cursor)
                cursor += 1
            }
            index = content.indexOf(startMark, cursor)
        }
        if (cursor < content.length) {
            prepareSQL.append(content.substring(cursor))
        }
        return prepareSQL.toString()
    }

    private fun getQueryFieldsFromPreparedStatement(connection: Connection, sql: String): StreamDataTypes {
        connection.use {
            it.prepareStatement(
                stripCR(sql), ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY
            ).use { preparedStatement ->
                preparedStatement.maxRows = 1

                val rsmd = preparedStatement.metaData
                return getRowInfo(rsmd)
            }
        }

    }

    private fun stripCR(sbsql: String?): String? {
        return if (sbsql == null) {
            null
        } else stripCR(StringBuilder(sbsql))
    }

    private fun stripCR(sbsql: StringBuilder): String {
        // DB2 Can't handle \n in SQL Statements...
        // Remove CR's
        for (i in sbsql.length - 1 downTo 0) {
            if (sbsql[i] == '\n' || sbsql[i] == '\r') {
                sbsql.setCharAt(i, ' ')
            }
        }
        return sbsql.toString()
    }

    private fun getRowInfo(
        rm: ResultSetMetaData
    ): StreamDataTypes {
        val columns = mutableListOf<String>()
        val types = mutableListOf<Int>()
        for (i in 1..rm.columnCount) {
            val columnName = rm.getColumnName(i)
            columns.add(columnName)

            val fieldType = rm.getColumnType(i)
            types.add(fieldType)
        }

        return StreamDataTypes.of(columns.toTypedArray(), types.toTypedArray())
    }

    private fun getQueryFieldsFromDatabaseMetaData(
        connection: Connection,
        sql: String
    ): StreamDataTypes {
        val resultSet: ResultSet = connection.metaData.getColumns(
            "", "",
            sql, ""
        )
        val columns = mutableListOf<String>()
        val types = mutableListOf<Int>()

        while (resultSet.next()) {
            val name = resultSet.getString("COLUMN_NAME")
            val type = resultSet.getString("SOURCE_DATA_TYPE")

            columns.add(name)
            val dType = when (type) {
                "Integer" -> SQLTypes.INTEGER
                "Long" -> SQLTypes.INTEGER
                "Double" -> SQLTypes.DOUBLE
                "Number" -> SQLTypes.NUMERIC
                "BigDecimal" -> SQLTypes.DECIMAL
                "BigNumber" -> SQLTypes.BIGINT
                "String" -> SQLTypes.VARCHAR
                "Date" -> SQLTypes.DATE
                "Boolean" -> SQLTypes.BOOLEAN
                "Binary" -> SQLTypes.BINARY
                "Timestamp" -> SQLTypes.TIMESTAMP
                else -> SQLTypes.VARCHAR
            }
            types.add(dType)
        }

        return StreamDataTypes.of(columns.toTypedArray(), types.toTypedArray())
    }
}