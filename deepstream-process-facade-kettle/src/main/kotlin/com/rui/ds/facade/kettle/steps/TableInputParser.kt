package com.rui.ds.facade.kettle.steps

import com.rui.ds.common.Step
import com.rui.ds.facade.kettle.KettleStep
import com.rui.ds.facade.kettle.KettleStepParser
import com.rui.ds.steps.input.TableInputStep
import com.rui.ds.steps.input.TableInputStepMeta
import org.dom4j.Element

@KettleStep("TableInput")
class TableInputParser: KettleStepParser {
    override fun parse(element: Element): Step {
        val name = element.elementText("name")
        val dsName = element.elementText("connection")
        val selectSql = element.elementText("sql")

        val meta = TableInputStepMeta(
            dsName = dsName,
            inputSql = selectSql
        )

        return TableInputStep(name, meta)
    }
}