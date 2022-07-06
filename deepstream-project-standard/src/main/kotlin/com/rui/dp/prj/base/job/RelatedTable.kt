package com.rui.dp.prj.base.job

data class RelatedTable(val tableName: String, val tableFields: List<DataField>, val tableType: TableType) :
    java.io.Serializable {
    fun toTableSql(): String {
        val fields = tableFields.joinToString(separator = ",\n") {
            "`${it.fieldName}` ${it.fieldType}"
        }
        val hasKey = tableFields.any { it.isKey }
        val primaryKeys = tableFields.filter { it.isKey }.joinToString(separator = ",") {
            "`${it.fieldName}`"
        }

        if (hasKey) {
            return """
            CREATE TABLE $tableName (
                $fields ,
                PRIMARY KEY (${primaryKeys}) NOT ENFORCED
            ) WITH (
                $tableType
            )
        """.trimIndent()
        } else {
            return """
            CREATE TABLE $tableName (
                $fields
            ) WITH (
                $tableType
            )
        """.trimIndent()
        }
    }
}