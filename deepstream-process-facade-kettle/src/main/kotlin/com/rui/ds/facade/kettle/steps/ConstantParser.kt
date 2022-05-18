package com.rui.ds.facade.kettle.steps

import com.rui.ds.common.Step
import com.rui.ds.facade.kettle.KettleStep
import com.rui.ds.facade.kettle.KettleStepParser
import org.dom4j.Element

@KettleStep("Constant")
class ConstantParser : KettleStepParser {
    override fun parse(element: Element): Step {
        val name = element.elementText("name")
        TODO("Not yet implemented")
    }
}