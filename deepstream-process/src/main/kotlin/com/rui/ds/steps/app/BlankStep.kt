package com.rui.ds.steps.app

import com.rui.ds.ProcessContext
import com.rui.ds.common.DataContext
import com.rui.ds.common.TransformStep

class BlankStep(name:String, meta: DefaultMeta): TransformStep(name, meta) {
    override fun process(data: DataContext, process: ProcessContext): DataContext {
        return data
    }
}