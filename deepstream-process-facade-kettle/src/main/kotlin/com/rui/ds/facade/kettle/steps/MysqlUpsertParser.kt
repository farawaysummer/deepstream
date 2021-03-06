package com.rui.ds.facade.kettle.steps

import com.rui.ds.common.Step
import com.rui.ds.common.TableContext
import com.rui.ds.facade.kettle.KettleStep
import com.rui.ds.facade.kettle.KettleStepParser
import com.rui.ds.steps.output.TableMergeIntoStep
import com.rui.ds.steps.output.TableMergeIntoStepMeta
import org.dom4j.Element

@KettleStep("MySQLUpsert")
class MysqlUpsertParser: KettleStepParser {
    override fun parse(element: Element): Step {
        val name = element.elementText("name")
        val dsName = element.elementText("connection")
        val lookupElement = element.element("lookup")
        val table = TableContext(
            catalog = lookupElement.elementText("schema"),
            tableName = lookupElement.elementText("table"),
            tableType = TableContext.TABLE_TYPE_SINK
        )

        val values = lookupElement.elements("value")
        val outputFields = values.map { it.elementText("name") }

        val meta = TableMergeIntoStepMeta(
            dsName = dsName,
            toTable = table,
            condition = emptyList(),
            outputFields = outputFields
        )

        return TableMergeIntoStep(name, meta)
    }
}