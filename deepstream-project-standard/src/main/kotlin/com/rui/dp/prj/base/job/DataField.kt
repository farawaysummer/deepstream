package com.rui.dp.prj.base.job

data class DataField(
    val fieldName: String,
    val fieldType: String,
    val isKey: Boolean
) : java.io.Serializable