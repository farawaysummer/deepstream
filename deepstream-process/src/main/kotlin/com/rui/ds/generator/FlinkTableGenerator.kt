package com.rui.ds.generator

import com.google.common.base.Joiner
import com.rui.ds.common.DataSourceConfig
import com.rui.ds.datasource.DatabaseSources

abstract class FlinkTableGenerator(val dsConfig: DataSourceConfig, private val tablePrefix: String = "") {

    abstract val typeMap: Map<String, String>

    abstract val tableType: String

    internal open fun createTableFields(fields: Map<String, String>): List<String> {

        val list = fields.map { (columnName, columnType) ->
            val transType = typeMap[columnType] ?: columnType
            val desc = "$columnName $transType"

            desc
        }.toList()

        return list
    }

    internal abstract fun createConnectorInfo(dbName: String, tableName: String): Map<String, Any>

    fun createTableSQL(
        dbName: String? = null,
        tableName: String,
        primaryKeys: List<String> = emptyList(),
        withProcTime: Boolean = false,
        partitionedBy: List<String> = emptyList()
    ): String {
        val connection = DatabaseSources.getConnection(dsConfig.name)!!
        val dbMetaData = connection.metaData

        //TODO get primary key by jdbc metadata
        dbMetaData.getPrimaryKeys(dbName, null, tableName)

        val tableResult = dbMetaData.getTables(dbName, null, tableName, arrayOf("TABLE"))
        while (tableResult.next()) {
            val tName = tableResult.getString("TABLE_NAME")

            val createSQL = StringBuilder("CREATE TABLE $tablePrefix$tName (")

            val columnResult = dbMetaData.getColumns(dbName, null, tName, null)
            val fields = linkedMapOf<String, String>()
            while (columnResult.next()) {
                val columnType = columnResult.getString("TYPE_NAME")
                val columnName = columnResult.getString("COLUMN_NAME")
                fields[columnName] = columnType
            }

            val fieldDesc = createTableFields(fields)
            fieldDesc.forEach { createSQL.append("\n").append(it).append(",") }
            if (withProcTime) {
                createSQL.append("\nproctime AS PROCTIME()").append(",")
                createSQL.append("\nwTime AS NOW()").append(",")
                createSQL.append("WATERMARK FOR wTime AS wTime - INTERVAL '1' SECOND").append(",")
            }

            if (partitionedBy.isNotEmpty()) {
                createSQL.append("PARTITIONED BY (")
                    .append(Joiner.on(",").join(partitionedBy)).append(")")
            }

            if (primaryKeys.isNotEmpty()) {
                createSQL.append("\nPRIMARY KEY (")
                    .append(Joiner.on(",").join(primaryKeys))
                    .append(") NOT ENFORCED\n")
            }

            createSQL.setLength(createSQL.length - 1)
            createSQL.append("\n) \n WITH (\n")
                .append("'connector' = '$tableType',\n")

            val connectorInfo = createConnectorInfo(dbName ?: "", tName)
            for ((key, value) in connectorInfo) {
                createSQL.append("'$key' = '$value',\n")
            }
            createSQL.setLength(createSQL.length - 2)
            createSQL.append(")")

            println(createSQL)

            return createSQL.toString()
        }

        return ""
    }

    fun createTablesSQL(
        dbName: String? = null,
        withProcTime: Boolean = false
    ): Map<String, String> {
        val tableSqlMap = mutableMapOf<String, String>()
        val connection = DatabaseSources.getConnection(dsConfig.name)!!
        val dbMetaData = connection.metaData
        val tableResult = dbMetaData.getTables(dbName, null, "%", arrayOf("TABLE"))
        while (tableResult.next()) {
            val tName = tableResult.getString("TABLE_NAME")

            val createSQL = StringBuilder("CREATE TABLE $tablePrefix$tName (")

            val columnResult = dbMetaData.getColumns(dbName, null, tName, null)
            val fields = linkedMapOf<String, String>()
            while (columnResult.next()) {
                val columnType = columnResult.getString("TYPE_NAME")
                val columnName = columnResult.getString("COLUMN_NAME")
                fields[columnName] = columnType
            }

            val fieldDesc = createTableFields(fields)
            fieldDesc.forEach { createSQL.append("\n").append(it).append(",") }
            if (withProcTime) {
                createSQL.append("\nproctime AS PROCTIME()").append(",")
            }

            createSQL.setLength(createSQL.length - 1)
            createSQL.append("\n) \n WITH (\n")
                .append("'connector' = '$tableType',\n")

            val connectorInfo = createConnectorInfo(dbName ?: "", tName)
            for ((key, value) in connectorInfo) {
                createSQL.append("'$key' = '$value',\n")
            }
            createSQL.setLength(createSQL.length - 2)
            createSQL.append(")")

            tableSqlMap[tName] = createSQL.toString()
        }

        println(tableSqlMap)

        return tableSqlMap.toMap()
    }
}