package com.rui.ds.parser

import com.rui.ds.job.DeepStreamJob

interface JobMetaParser<T> {
    fun parse(source: T): DeepStreamJob?
}