package com.rui.ds.steps.input

import com.rui.ds.ProcessContext
import com.rui.ds.common.*
import com.rui.ds.generator.TableGenerator
import com.rui.ds.utils.TableSqlParser
import com.rui.ds.log.logger

class TableInputStep(name: String, override val meta: TableInputStepMeta) : InputStep(name, meta) {
    private val tables: List<TableContext> = TableSqlParser.extractQueryTables(meta.inputSql)

    init {
        logger().debug("解析表输入SQL: ${meta.inputSql}")
        logger().debug("解析的表定义包括: $tables")
    }

    override fun process(data: DataContext, process: ProcessContext): DataContext {
        // registry all tables
        tables.forEach {
            createTable(process, it)
        }

        val resultTable = process.tableEnv.sqlQuery(meta.inputSql)

        return dataByTable(resultTable)
    }

    private fun createTable(process: ProcessContext, tableContext: TableContext) {
        val tableEnv = process.tableEnv
        tableEnv.executeSql(
            TableGenerator.getGenerator(meta.dsName, tableContext.tableType).createTableSQL(
                dbName = tableContext.catalog,
                tableName = tableContext.tableName
            )
        )
    }
}

data class TableInputStepMeta(
    val dsName: String,
    val inputSql: String
) : StepMeta