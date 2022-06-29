package com.rui.ds.types

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeinfo.Types

class DatetimeTypeSupport : TypeSupport {
    override fun convert(fieldName: String, sourceType: TypeInformation<*>): String {
        val convertWord = when (sourceType) {
            Types.LOCAL_DATE_TIME -> "`$fieldName`"
            Types.STRING -> "TO_TIMESTAMP(`$fieldName`, '${DeepStreamTypes.DATE_TIME_FORMAT}')"
            Types.LOCAL_DATE -> "TO_TIMESTAMP(CAST(`$fieldName` AS STRING), '${DeepStreamTypes.DATE_TIME_FORMAT}')"
            Types.LOCAL_TIME -> "TO_TIMESTAMP(CAST(`$fieldName` AS STRING), '${DeepStreamTypes.DATE_TIME_FORMAT}')"

            else -> throw IllegalArgumentException(
                "Cannot convert $sourceType field '$fieldName' to Timestamp"
            )
        }

        return convertWord
    }
}