package com.rui.dp.prj.base

import com.rui.ds.ks.delay.DeepStreamDelay

object Consts {
    const val FIELD_PROC_TIME = "proctime"
    const val FIELD_DEAD_LINE = DeepStreamDelay.FIELD_DEAD_LINE
    const val RETRY_POLICY_CHECK_EMPTY = "checkEmpty"
    const val RETRY_POLICY_CHECK_REQUIRED = "checkRequired"

    const val VALUE_MAPPING_DS = "MC_DS"
    const val RESULT_TABLE = "DTable"
}