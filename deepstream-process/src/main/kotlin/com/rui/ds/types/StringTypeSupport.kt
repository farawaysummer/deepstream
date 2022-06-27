package com.rui.ds.types

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeinfo.Types

class StringTypeSupport : TypeSupport {

    override fun convert(fieldName: String, sourceType: TypeInformation<*>): String {
        val convertWord = when (sourceType) {
            Types.STRING -> fieldName
            Types.INT -> "CAST($fieldName AS STRING)"
            Types.BIG_DEC -> "CAST($fieldName AS STRING)"
            Types.LOCAL_DATE_TIME -> "DATE_FORMAT($fieldName, \"${DeepStreamTypes.DATE_TIME_FORMAT}\")"
            Types.LOCAL_DATE -> "DATE_FORMAT($fieldName, \"${DeepStreamTypes.DATE_FORMAT}\")"
            Types.LOCAL_TIME -> "DATE_FORMAT($fieldName, \"${DeepStreamTypes.TIME_FORMAT}\")"

            else -> throw IllegalArgumentException(
                "Cannot convert $sourceType field '$fieldName' to Boolean"
            )
        }

        return convertWord
    }

}