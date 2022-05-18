package com.rui.ds.facade.kettle.steps

import com.rui.ds.FieldInfo
import com.rui.ds.common.Step
import com.rui.ds.facade.kettle.KettleStep
import com.rui.ds.facade.kettle.KettleStepParser
import com.rui.ds.steps.transform.ScriptStep
import com.rui.ds.steps.transform.ScriptStepMeta
import org.dom4j.Element

@KettleStep("ScriptValueMod")
class JsScriptParser : KettleStepParser {
    override fun parse(element: Element): Step {
        val name = element.elementText("name")
        val scriptsElement = element.element("jsScripts")
        val scriptElement = scriptsElement.element("jsScript")
        val script = scriptElement.elementText("jsScript_script")
        val fieldsElement = element.element("fields")
        val outputFields = fieldsElement.elements("field")
            .map { FieldInfo(it.elementText("name"), it.elementText("type")) }

        val meta = ScriptStepMeta(
            script = script,
            outputFields = outputFields
        )

        return ScriptStep(name, meta)
    }
}