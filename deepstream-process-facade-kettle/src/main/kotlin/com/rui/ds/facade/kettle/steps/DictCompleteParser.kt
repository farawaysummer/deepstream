package com.rui.ds.facade.kettle.steps

import com.rui.ds.common.Step
import com.rui.ds.facade.kettle.KettleStep
import com.rui.ds.facade.kettle.KettleStepParser
import com.rui.ds.steps.transform.DictCompleteStepMeta
import org.dom4j.Element

@KettleStep("DictComplement")
class DictCompleteParser: KettleStepParser {
    override fun parse(element: Element): Step {
        val name = element.elementText("name")

//        val meta = DictCompleteStepMeta(
//
//        )
        TODO("Not yet implemented")
    }
}