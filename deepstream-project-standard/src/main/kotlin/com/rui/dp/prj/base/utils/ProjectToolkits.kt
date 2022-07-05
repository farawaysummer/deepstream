package com.rui.dp.prj.base.utils

import com.google.common.collect.Sets
import com.rui.dp.prj.base.DeepStreamHelper
import com.rui.ds.datasource.DatabaseSources
import com.rui.ds.facade.kettle.KettleJobParser
import com.rui.ds.types.DeepStreamTypes
import org.dom4j.io.SAXReader

object ProjectToolkits {

    init {
        loadDatasource()
    }

    @JvmStatic
    fun loadDatasource() {
        val inputStream = javaClass.getResourceAsStream("/data_source.xml")!!

        // read from xml
        val reader = SAXReader()
        val document = reader.read(inputStream)
        val rootElement = document.rootElement

        val connections = rootElement.elements("connection")

        val configs = connections.map {
            KettleJobParser.parseConnection(it)
        }

        configs.forEach {
            DatabaseSources.registryDataSource(it)
        }
    }

    @JvmStatic
    fun generateTableFields(dsName: String, dbName: String, tableName: String): String {
        val fieldContent = java.lang.StringBuilder()
        val connection = DatabaseSources.getConnection(dsName)!!
        connection.use {
            val dbMetaData = connection.metaData

            //TODO get primary key by jdbc metadata
            val schemaName = dbName.ifBlank {
                null
            }

            val sourcePKSet = dbMetaData.getPrimaryKeys(schemaName, null, tableName)
            val sourcePK = mutableSetOf<String>()
            while (sourcePKSet.next()) {
                sourcePK.add(sourcePKSet.getString("COLUMN_NAME"))
            }

            val tableResult = dbMetaData.getTables(schemaName, null, tableName, arrayOf("TABLE"))
            while (tableResult.next()) {

                val columnResult = dbMetaData.getColumns(schemaName, null, tableName, null)
                val fields = linkedMapOf<String, String>()
                while (columnResult.next()) {
                    val columnType = columnResult.getString("TYPE_NAME")
                    val columnName = columnResult.getString("COLUMN_NAME")

                    fields[columnName] = columnType
                    fieldContent.append("<field name=\"$columnName\" type=\"$columnType\" isKey=\"${sourcePK.contains(columnName)}\"/>\n")
                }

            }

            return fieldContent.toString()
        }
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
                val type = mappingTypeNameToNormalize(metaData.getColumnTypeName(index))

                fields[name] = type

                fieldContent.append("<field name=\"$name\" type=\"$type\"/>\n")
            }
        }

        return fieldContent.toString()
    }

    private fun mappingTypeNameToNormalize(typeName: String): String {
        return when (typeName) {
            "VARCHAR" -> DeepStreamTypes.DATA_TYPE_STRING
            "VARCHAR2" -> DeepStreamTypes.DATA_TYPE_STRING
            "CHAR" -> DeepStreamTypes.DATA_TYPE_STRING
            "TIMESTAMP" -> DeepStreamTypes.DATA_TYPE_TIMESTAMP
            "DATE" -> DeepStreamTypes.DATA_TYPE_DATE
            "TIME" -> DeepStreamTypes.DATA_TYPE_TIME
            "NUMBER" -> DeepStreamTypes.DATA_TYPE_STRING
            else -> DeepStreamTypes.DATA_TYPE_STRING
        }
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
                val fields = mutableMapOf<String, String>()
                while (columnResult.next()) {
                    val columnName = columnResult.getString("COLUMN_NAME")
                    val columnType = columnResult.getString("TYPE_NAME")
                    fields[columnName] = columnType
                }

                val insertColumns = Sets.intersection(fields.keys, sourceFields.keys)

                val functions = insertColumns.map {
                    val sourceType = DeepStreamHelper.mappingTypeNameToInformation(sourceFields[it]!!)
                    val tarTypeName = DeepStreamHelper.mappingTypeNameToNormalize(fields[it]!!)
                    DeepStreamTypes.convertFunction(it, sourceType, tarTypeName)
                }

                val insertFieldStr = insertColumns.joinToString(separator = ",") { "`$it`" }
//                val fromFieldStr = insertColumns.joinToString(separator = ",\n") { "CAST(`$it` AS STRING)" }
                val fromFieldStr = functions.joinToString(separator = ",\n")
                // cast function

                return "INSERT INTO $tableName ($insertFieldStr) \n" +
                        " SELECT $fromFieldStr FROM DTable"
            }

            return ""
        }
    }

}
