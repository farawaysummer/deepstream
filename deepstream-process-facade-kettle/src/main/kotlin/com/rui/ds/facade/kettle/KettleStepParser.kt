package com.rui.ds.facade.kettle

import com.rui.ds.common.Step
import org.dom4j.Element

interface KettleStepParser {
    fun parse(element: Element): Step
}

@Repeatable
annotation class KettleStep(
    val name: String
)