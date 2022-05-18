package com.rui.ds.parser

import com.rui.ds.common.StepMeta

interface StepMetaParser<T> {
    fun parse(source: T): StepMeta
}