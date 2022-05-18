package com.rui.ds.facade.kettle.steps

import com.rui.ds.common.Step
import com.rui.ds.facade.kettle.KettleStep
import com.rui.ds.facade.kettle.KettleStepParser
import com.rui.ds.steps.transform.FieldSelectStep
import com.rui.ds.steps.transform.FieldSelectStepMeta
import org.dom4j.Element

@KettleStep("SelectValues")
class FieldSelectParser: KettleStepParser {
    override fun parse(element: Element): Step {
        val name = element.elementText("name")
        val fieldsElement = element.element("fields")

        val removesElement = fieldsElement.elements("remove")
        val dropFields = removesElement.map { it.elementText("name") }

        val meta = FieldSelectStepMeta(
            mapOf(),
            dropFields
        )

        return FieldSelectStep(name, meta)
    }
}