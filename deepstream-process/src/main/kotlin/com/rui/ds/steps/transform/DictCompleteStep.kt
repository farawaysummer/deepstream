package com.rui.ds.steps.transform

import com.rui.ds.ProcessContext
import com.rui.ds.common.DataContext
import com.rui.ds.common.StepMeta
import com.rui.ds.common.TransformStep


class DictCompleteStep(name: String, override val meta: DictCompleteStepMeta) : TransformStep(name, meta) {
    // TODO 实现值域映射
    override fun process(data: DataContext, process: ProcessContext): DataContext {
        TODO("Not yet implemented")
    }
}

data class DictCompleteStepMeta(
    val system: String
) : StepMeta