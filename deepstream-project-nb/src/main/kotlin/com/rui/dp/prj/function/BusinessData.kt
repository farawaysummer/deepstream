package com.rui.dp.prj.function

data class BusinessData(
    val businessName: String,
    val dsName: String,
    val businessSql: String,
    val conditionFields: List<String>,
    val resultFields: Map<String, String>
)