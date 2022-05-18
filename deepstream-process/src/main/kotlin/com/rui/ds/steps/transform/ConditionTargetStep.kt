package com.rui.ds.steps.transform

import com.rui.ds.ProcessContext
import com.rui.ds.common.DataContext
import com.rui.ds.common.StepMeta
import com.rui.ds.common.TransformStep
import com.rui.ds.steps.funs.ConditionStepFunction

class ConditionTargetStep(name: String, override val meta: ConditionTargetStepMeta) : TransformStep(name, meta) {
    private val checkFunction: ConditionStepFunction = ConditionStepFunction(
        meta.conditionField,
        meta.hops,
        meta.defaultHop
    )

    override fun process(data: DataContext, process: ProcessContext): DataContext {
        val stream = toStream(data, process)!!

        val selected = stream.process(checkFunction)
        val result = dataByStream(selected)

        result.outputTags = checkFunction.outputTags

        return result
    }
}

data class ConditionTargetStepMeta(
    val conditionField: String,
    val hops: Map<String, String>,
    val defaultHop: String
) : StepMeta


