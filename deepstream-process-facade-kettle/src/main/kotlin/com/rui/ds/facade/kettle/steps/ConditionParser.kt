package com.rui.ds.facade.kettle.steps

import com.rui.ds.common.Step
import com.rui.ds.facade.kettle.KettleStep
import com.rui.ds.facade.kettle.KettleStepParser
import com.rui.ds.steps.transform.ConditionTargetStep
import com.rui.ds.steps.transform.ConditionTargetStepMeta
import org.dom4j.Element

@KettleStep("SwitchCase")
class ConditionParser : KettleStepParser {
    override fun parse(element: Element): Step {
        val name = element.elementText("name")
        val fieldName = element.elementText("fieldname")
        val defaultTarget = element.elementText("default_target_step")
        val casesElement = element.element("cases")
        val cases = casesElement.elements("case")
        val targets = cases.associateBy(
            { it.elementText("value") }, { it.elementText("target_step") }
        )

        val meta = ConditionTargetStepMeta(
            conditionField = fieldName,
            defaultHop = defaultTarget,
            hops = targets
        )

        return ConditionTargetStep(name, meta)
    }
}