package com.rui.dp.prj.utils

import com.google.common.collect.Sets
import com.rui.dp.prj.base.DeepStreamHelper
import com.rui.ds.datasource.DatabaseSources
import com.rui.ds.generator.JdbcDimTableGenerator

object ProjectToolkits {

    init {
        DeepStreamHelper.loadDatasource()
        DeepStreamHelper.loadSql()
    }

    @JvmStatic
    fun getSql(name: String): String {
        return DeepStreamHelper.getSql(name)!!
    }

    @JvmStatic
    fun generateFlinkTableSql(dsName: String, dbName: String, tableName: String): String {
        val dsConfig = DatabaseSources.getDataSourceConfig(dsName)!!
        return JdbcDimTableGenerator(dsConfig).createTableSQL(
            dbName = dbName,
            tableName = tableName
        )
    }

    @JvmStatic
    fun generateResultFields(dsName: String, sql: String, fields: MutableMap<String, String> = mutableMapOf()): String {
        val fieldContent = java.lang.StringBuilder()
        DatabaseSources.getConnection(dsName).use { connection ->
            val statement = connection!!.prepareStatement(sql)

            val result = statement.executeQuery()

            val metaData = result.metaData
            for (index in 1..metaData.columnCount) {
                val name = metaData.getColumnName(index)
                val type = metaData.getColumnTypeName(index)

                fields[name] = type

                fieldContent.append("<field name=\"$name\" type=\"$type\"/>\n")
            }
        }

        return fieldContent.toString()
    }

    @JvmStatic
    fun generateInsertSql(
        targetDs: String,
        schemaName: String,
        tableName: String,
        sourceFields: Map<String, String>
    ): String {
        val connection = DatabaseSources.getConnection(targetDs)!!
        connection.use {
            val dbMetaData = connection.metaData

            val tableResult = dbMetaData.getTables(schemaName, null, tableName, arrayOf("TABLE"))
            while (tableResult.next()) {
                val tName = tableResult.getString("TABLE_NAME").uppercase()
                val columnResult = dbMetaData.getColumns(schemaName, null, tName, null)
                val fields = mutableSetOf<String>()
                while (columnResult.next()) {
                    val columnName = columnResult.getString("COLUMN_NAME")
                    fields.add(columnName)
                }

                val insertColumns = Sets.intersection(fields, sourceFields.keys)

                val insertFieldStr = insertColumns.joinToString(separator = ",") {"`$it`"}
//                val fromFieldStr = insertColumns.joinToString(separator = ",\n") { "CAST(`$it` AS STRING)" }
                val fromFieldStr = insertColumns.joinToString(separator = ",\n") { "`$it`" }
                // cast function

                val insertSql = "INSERT INTO $tableName ($insertFieldStr) \n" +
                        " SELECT $fromFieldStr FROM DTable"

                return insertSql
            }

            return ""
        }
    }

}
