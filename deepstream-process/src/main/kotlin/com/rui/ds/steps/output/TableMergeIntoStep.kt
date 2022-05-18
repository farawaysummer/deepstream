package com.rui.ds.steps.output

import com.rui.ds.ProcessContext
import com.rui.ds.common.DataContext
import com.rui.ds.common.OutputStep
import com.rui.ds.common.StepMeta
import com.rui.ds.common.TableContext
import com.rui.ds.generator.TableGenerator
import io.debezium.util.Joiner

class TableMergeIntoStep(name: String ,override val meta: TableMergeIntoStepMeta): OutputStep(name, meta) {

    override fun process(data: DataContext, process: ProcessContext): DataContext {
        val table = toTable(data, process)

        // create table
        createTable(process, meta.toTable)

        process.tableEnv.createTemporaryView("DTable", table)

        // insert from table
        val insertSql = """
            UPSERT INTO ${meta.toTable.tableName} (${Joiner.on(",").join(meta.outputFields)})
                SELECT ${Joiner.on(",").join(meta.outputFields)} FROM DTable
            """.trimIndent()

        process.tableEnv.executeSql(insertSql)

        return data
    }

    private fun createTable(process: ProcessContext, tableContext: TableContext) {
        val tableEnv = process.tableEnv
        tableEnv.executeSql(
            TableGenerator.getGenerator(meta.dsName, tableContext.tableType).createTableSQL(
                dbName = tableContext.catalog,
                tableName = tableContext.tableName,
                primaryKeys = meta.condition
            )
        )
    }
}

data class TableMergeIntoStepMeta(
    val dsName: String,
    val toTable: TableContext,
    val outputFields: List<String>,
    val condition: List<String>
): StepMeta