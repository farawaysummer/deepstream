package com.rui.dp.prj.base.funs.vp

import com.ruisoft.eig.transform.TransformConfig
import com.ruisoft.eig.transform.transformer.TransformFailStrategy

enum class DPTransformConfig : TransformConfig {
    INSTANCE;

    override fun getValueDomainFailStrategy(): TransformFailStrategy {
        return TransformFailStrategy.FAIL_NO_TRANS
    }

    override fun isValueDomainSmartMapOn(): Boolean {
        return true
    }

    override fun getValueDomainReloadIntervalInMinute(): Int {
        return 120
    }
}