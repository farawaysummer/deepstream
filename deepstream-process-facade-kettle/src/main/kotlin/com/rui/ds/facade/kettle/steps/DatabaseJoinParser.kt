package com.rui.ds.facade.kettle.steps

import com.rui.ds.common.Step
import com.rui.ds.facade.kettle.KettleStep
import com.rui.ds.facade.kettle.KettleStepParser
import com.rui.ds.steps.query.DatabaseJoinStep
import com.rui.ds.steps.query.DatabaseJoinStepMeta
import org.dom4j.Element

@KettleStep("DBJoin")
class DatabaseJoinParser : KettleStepParser {
    override fun parse(element: Element): Step {
        val name = element.elementText("name")
        val sql = element.elementText("sql")
        val dsName = element.elementText("connection")
        val rowLimit = element.elementText("rowlimit")
        val conditionsElement = element.element("parameter")
        val conditionFields =
            conditionsElement.elements("field")
                .map { it.elementText("name") }

        val meta = DatabaseJoinStepMeta(
            sql = sql,
            dsName = dsName,
            recordLimit = rowLimit.toInt(),
            joinFields = conditionFields
        )

        return DatabaseJoinStep(name, meta)
    }
}