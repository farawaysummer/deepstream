package com.rui.ds.steps.transform

import com.rui.ds.FieldInfo
import com.rui.ds.ProcessContext
import com.rui.ds.StreamDataTypes
import com.rui.ds.common.DataContext
import com.rui.ds.common.StepMeta
import com.rui.ds.common.TransformStep
import com.rui.ds.steps.funs.AddConstantFunction

class ConstantStep(name: String, override val meta: ConstantStepMeta): TransformStep(name, meta) {
    private val constantFunction = AddConstantFunction(
        meta.fields
    )

    override fun process(data: DataContext, process: ProcessContext): DataContext {
        // 转换的策略
        val stream = toStream(data, process)!!

        return dataByStream(stream.map(constantFunction), processDataType(data))
    }

    private fun processDataType(data: DataContext): StreamDataTypes {
        var originTypes = data.types
//        if (meta.selectedFields.isNotEmpty()) {
//            originTypes = originTypes.rename(meta.selectedFields)
//                .select(meta.selectedFields.keys.toTypedArray())
//        }
//
//        originTypes = originTypes.drop(meta.dropFields.toTypedArray())

        return originTypes
    }
}

data class ConstantStepMeta(
    val fields: Map<FieldInfo, Any>
): StepMeta