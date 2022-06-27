package com.rui.ds.types

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeinfo.Types

object DeepStreamTypes {
    val supportTypes: Array<TypeInformation<*>> = arrayOf(
        Types.STRING, Types.LOCAL_DATE_TIME, Types.LOCAL_DATE, Types.LOCAL_TIME, Types.BIG_DEC, Types.INT
    )

    val DATE_FORMAT: String = System.getProperty("dp.date_format", "yyyy-MM-dd")
    val TIME_FORMAT: String = System.getProperty("dp.time_format", "HH:mm:ss")
    val DATE_TIME_FORMAT: String = System.getProperty("dp.datetime_format", "yyyy-MM-dd HH:mm:ss")

    const val DATA_TYPE_DECIMAL: String = "DECIMAL"
    const val DATA_TYPE_STRING: String = "STRING"
    const val DATA_TYPE_INTEGER: String = "INTEGER"
    const val DATA_TYPE_DATE: String = "DATE"
    const val DATA_TYPE_TIME: String = "TIME"
    const val DATA_TYPE_TIMESTAMP: String = "TIMESTAMP"

    val typeSupports: Map<String, TypeSupport> = mapOf(
        DATA_TYPE_STRING to StringTypeSupport(),
        DATA_TYPE_DECIMAL to BigDecimalTypeSupport(),
        DATA_TYPE_INTEGER to IntTypeSupport(),
        DATA_TYPE_DATE to DateTypeSupport(),
        DATA_TYPE_TIMESTAMP to DatetimeTypeSupport()
    )

    fun convertFunction(fieldName: String, sourceType: TypeInformation<*>, targetType: String): String {
        val supporter = typeSupports[targetType] ?: typeSupports[DATA_TYPE_STRING]!!
        return supporter.convert(fieldName, sourceType)
    }
}