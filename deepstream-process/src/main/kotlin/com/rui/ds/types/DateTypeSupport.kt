package com.rui.ds.types

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeinfo.Types

class DateTypeSupport: TypeSupport {
    override fun convert(fieldName: String, sourceType: TypeInformation<*>): String {
        val convertWord = when (sourceType) {
            Types.LOCAL_DATE -> "`$fieldName`"
            Types.STRING -> "TO_DATE(`$fieldName`, '${DeepStreamTypes.DATE_FORMAT}')"
            Types.LOCAL_DATE_TIME -> "TO_DATE(DATE_FORMAT($fieldName, '${DeepStreamTypes.DATE_FORMAT}'), '${DeepStreamTypes.DATE_FORMAT}')"

            else -> throw IllegalArgumentException(
                "Cannot convert $sourceType field '$fieldName' to Date"
            )
        }

        return convertWord
    }
}