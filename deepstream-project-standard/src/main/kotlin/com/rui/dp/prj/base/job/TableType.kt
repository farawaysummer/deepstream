package com.rui.dp.prj.base.job

data class TableType(val type: String, val properties: Map<String, String>, val ignoreKey: Boolean = false): java.io.Serializable {

    override fun toString(): String {
        val defStr = properties.entries.joinToString(separator = ",\n") {
            "'${it.key}' = '${it.value}'"
        }

        return """
            'connector' = '$type',
            $defStr
        """.trimIndent()
    }
}