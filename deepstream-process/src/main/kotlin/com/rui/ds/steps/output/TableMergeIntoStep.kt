package com.rui.ds.steps.output

import com.rui.ds.ProcessContext
import com.rui.ds.common.DataContext
import com.rui.ds.common.OutputStep
import com.rui.ds.common.StepMeta
import com.rui.ds.common.TableContext
import com.rui.ds.generator.TableGenerator
import org.apache.flink.table.api.Table
import com.rui.ds.log.logger
import io.debezium.util.Joiner
import org.apache.flink.api.common.typeinfo.TypeInformation

class TableMergeIntoStep(name: String ,override val meta: TableMergeIntoStepMeta): OutputStep(name, meta) {

    override fun process(data: DataContext, process: ProcessContext): DataContext {
        val table = toTable(data, process)

        // create table
        createTable(process, meta.toTable)

        process.tableEnv.createTemporaryView("DTable", table)

        // insert from table
        // TODO 能否支持UPSERT INTO？
        // todo 根据源与目标表列定义的类型区别，生成带有转换过程的插入语句
        val insertSql = generateInsertSql(table!!, process)

        logger().debug("更新记录SQL:\n $insertSql")

        process.tableEnv.executeSql(insertSql)

        return data
    }

    private fun generateInsertSql(sourceTable: Table, process: ProcessContext): String {
        //比较源与目标表的字段类型区别
        val sourceTypes = dataTypes(sourceTable.resolvedSchema)
        val targetTypes = dataTypes(process.tableEnv.from(meta.toTable.tableName).resolvedSchema)
        for (field in meta.outputFields) {
            val sourceFieldType = sourceTypes.fieldType(field)
            val targetFieldType = targetTypes.fieldType(field)
        }

        val insertSql = """
            INSERT INTO ${meta.toTable.tableName.uppercase()} (${Joiner.on(",").join(meta.outputFields)})
                SELECT ${Joiner.on(",").join(meta.outputFields)} FROM DTable
            """.trimIndent()



        TODO()
    }

    private fun checkTypeMatch(sourceType: TypeInformation<*>, targetType: TypeInformation<*>): Boolean {
        if (sourceType == targetType) {
            return true
        }

        return false
    }

    private fun createTable(process: ProcessContext, tableContext: TableContext) {
        val tableEnv = process.tableEnv
        tableEnv.executeSql(
            TableGenerator. getGenerator(meta.dsName, tableContext.tableType).createTableSQL(
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