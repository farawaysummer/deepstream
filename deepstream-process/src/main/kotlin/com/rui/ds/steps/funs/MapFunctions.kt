package com.rui.ds.steps.funs

import com.rui.ds.FieldInfo
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.types.Row

class FieldSelectFunction(
    private val selectedFields: Map<String, String>,
    private val dropFields: Array<String>
) : MapFunction<Row, Row> {

    override fun map(value: Row): Row {
        val newRow = Row.withNames(value.kind)
        if (selectedFields.isEmpty()) {
            val newFields = value.getFieldNames(true)!!.filter { !dropFields.contains(it) }
            for (field in newFields) {
                newRow.setField(field, value.getField(field))
            }
        } else {
            for ((fromName, toName) in selectedFields) {
                if (!dropFields.contains(fromName)) {
                    newRow.setField(toName, value.getField(fromName))
                }
            }
        }

        return newRow
    }

}

class AddConstantFunction(
    private val fields: Map<FieldInfo, Any>
) : MapFunction<Row, Row> {
    override fun map(value: Row?): Row {
        TODO("Not yet implemented")
    }
}