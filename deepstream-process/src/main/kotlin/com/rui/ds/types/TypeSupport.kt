package com.rui.ds.types

import org.apache.flink.api.common.typeinfo.TypeInformation

interface TypeSupport {
    fun convert(fieldName: String, sourceType: TypeInformation<*>): String
}