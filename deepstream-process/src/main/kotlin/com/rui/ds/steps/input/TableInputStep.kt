package com.rui.ds.steps.input

import com.rui.ds.ProcessContext
import com.rui.ds.common.DataContext
import com.rui.ds.common.InputStep
import com.rui.ds.common.StepMeta
import com.rui.ds.common.TableContext
import com.rui.ds.generator.TableGenerator
import com.rui.ds.utils.TableSqlParser

class TableInputStep(name: String, override val meta: TableInputStepMeta): InputStep(name, meta) {
    private val tables: List<TableContext> = TableSqlParser.extractQueryTables(meta.inputSql)

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
): StepMeta