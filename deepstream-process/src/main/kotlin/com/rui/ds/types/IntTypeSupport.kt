package com.rui.ds.types

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeinfo.Types

class IntTypeSupport: TypeSupport {
    override fun convert(fieldName: String, sourceType: TypeInformation<*>): String {
        val convertWord = when (sourceType) {
            Types.INT -> fieldName
            Types.STRING -> "CAST($fieldName AS INTEGER)"
            Types.BIG_DEC -> "CAST($fieldName AS STRING)"

            else -> throw IllegalArgumentException(
                "Cannot convert $sourceType field '$fieldName' to Int"
            )
        }

        return convertWord
    }
}