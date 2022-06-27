package com.rui.ds.types

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeinfo.Types

class BigDecimalTypeSupport: TypeSupport {
    override fun convert(fieldName: String, sourceType: TypeInformation<*>): String {
        val convertWord = when (sourceType) {
            Types.BIG_DEC -> fieldName
            Types.STRING -> "CAST($fieldName AS DECIMAL)"
            Types.INT -> "CAST($fieldName AS DECIMAL)"
            Types.BIG_INT -> "CAST($fieldName AS DECIMAL)"
            Types.DOUBLE -> "CAST($fieldName AS DECIMAL)"

            else -> throw IllegalArgumentException(
                "Cannot convert $sourceType field '$fieldName' to Int"
            )
        }

        return convertWord
    }
}