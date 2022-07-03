package com.rui.dp.prj.base

data class BusinessData(
    val businessName: String,
    val dsName: String,
    val businessSql: String,
    val dictTransformNames: List<String> = mutableListOf(),
    val relatedTables: List<String>,
    val conditionFields: List<String>,
    val resultFields: Map<String, String>
): java.io.Serializable

