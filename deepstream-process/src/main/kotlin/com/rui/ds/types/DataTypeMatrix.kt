package com.rui.ds.types

import com.google.common.collect.HashBasedTable
import com.google.common.collect.Table

class DataTypeMatrix {
    private val typeTable: Table<String, String, String> = HashBasedTable.create()

    init {
        typeTable.put("String", "Instant", "to_date")
        typeTable.put("LocalDateTime", "String", "date_to_str")
        typeTable.put("Integer", "String", "")
    }
}