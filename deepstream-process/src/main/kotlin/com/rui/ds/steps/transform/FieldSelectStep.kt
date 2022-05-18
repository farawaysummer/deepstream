package com.rui.ds.steps.transform

import com.rui.ds.ProcessContext
import com.rui.ds.StreamDataTypes
import com.rui.ds.common.DataContext
import com.rui.ds.common.StepMeta
import com.rui.ds.common.TransformStep
import com.rui.ds.steps.funs.FieldSelectFunction
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.types.Row

class FieldSelectStep(name:String, override val meta: FieldSelectStepMeta) : TransformStep(name, meta) {

    private val selectFunction: MapFunction<Row, Row> = FieldSelectFunction(
        meta.selectedFields,
        meta.dropFields.toTypedArray()
    )

    override fun process(data: DataContext, process: ProcessContext): DataContext {
        // 转换的策略
        val stream = toStream(data, process)!!

        return dataByStream(stream.map(selectFunction), processDataType(data))
    }

    private fun processDataType(data: DataContext): StreamDataTypes {
        var originTypes = data.types
        if (meta.selectedFields.isNotEmpty()) {
            originTypes = originTypes.rename(meta.selectedFields)
                .select(meta.selectedFields.keys.toTypedArray())
        }

        originTypes = originTypes.drop(meta.dropFields.toTypedArray())

        return originTypes
    }
}

data class FieldSelectStepMeta(
    val selectedFields: Map<String, String>,
    val dropFields: List<String>
) : StepMeta