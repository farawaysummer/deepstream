package com.rui.ds.facade.kettle.steps

import com.rui.ds.common.Step
import com.rui.ds.facade.kettle.KettleStep
import com.rui.ds.facade.kettle.KettleStepParser
import com.rui.ds.steps.app.BlankStep
import com.rui.ds.steps.app.DefaultMeta
import org.dom4j.Element

@KettleStep("EIGTransform")
class BlankParser: KettleStepParser {

    override fun parse(element: Element): Step {
        val name = element.elementText("name")

        return BlankStep(name, DefaultMeta())
    }
}