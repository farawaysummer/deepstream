package com.rui.ds.steps.output

import com.rui.ds.ProcessContext
import com.rui.ds.common.DataContext
import com.rui.ds.common.OutputStep
import com.rui.ds.common.StepMeta
import com.rui.ds.common.TableContext
import com.rui.ds.generator.TableGenerator
import org.apache.flink.table.api.Table
import com.rui.ds.log.logger
import org.apache.flink.api.common.typeinfo.IntegerTypeInfo
import org.apache.flink.api.common.typeinfo.TypeInformation

class TableMergeIntoStep(name: String, override val meta: TableMergeIntoStepMeta) : OutputStep(name, meta) {

    override fun process(data: DataContext, process: ProcessContext): DataContext {
        val table = toTable(data, process)

        // create table
        createTable(process, meta.toTable)

        process.tableEnv.createTemporaryView("DTable", table)

        // insert from table
        val insertSql = generateInsertSql(table!!, process)

        process.tableEnv.executeSql(insertSql)

        return data
    }

    private fun generateInsertSql(sourceTable: Table, process: ProcessContext): String {
        //比较源与目标表的字段类型区别
        val sourceTypes = dataTypes(sourceTable.resolvedSchema)
        val targetTypes = dataTypes(process.tableEnv.from(meta.toTable.tableName).resolvedSchema)
        val selectFields = meta.outputFields.joinToString(separator = ",") { field ->
            val sourceFieldType = sourceTypes.fieldType(field)
            val targetFieldType = targetTypes.fieldType(field)!!
//
            if (!checkTypeMatch(sourceFieldType, targetFieldType)) {
                // 生成类型转换语法
                typeConvert(field, toTypeName(targetFieldType))
            } else {
                field
            }
        }

        val insertFields = meta.outputFields.joinToString(
            separator = ",",
            prefix = "`",
            postfix = "`"
        )

        val insertSql = """
            INSERT INTO ${meta.toTable.tableName.uppercase()} ($insertFields)
                SELECT $selectFields FROM DTable
            """.trimIndent()

        logger().info("更新记录SQL:\n $insertSql")

        return insertSql
    }

    private fun checkTypeMatch(sourceType: TypeInformation<*>?, targetType: TypeInformation<*>?): Boolean {
        if (sourceType == targetType || sourceType == null) {
            return true
        }

        return false
    }

    private fun toTypeName(type: TypeInformation<*>): String {
        val typeStr = when (type) {
            is IntegerTypeInfo -> "INTEGER"

            else -> "STRING"
        }

        return typeStr
    }

    private fun typeConvert(fieldName: String, typeName: String): String {
        return "TRY_CAST($fieldName AS $typeName)"
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
) : StepMeta