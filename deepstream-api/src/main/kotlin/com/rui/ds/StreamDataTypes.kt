package com.rui.ds

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.table.runtime.types.TypeInfoDataTypeConverter
import org.apache.flink.table.types.DataType
import org.apache.flink.types.Row
import java.lang.Integer.min

typealias SQLTypes = java.sql.Types

data class StreamDataTypes internal constructor(
    val fields: Array<String>,
    val types: Array<TypeInformation<*>>
) {
    override fun equals(other: Any?): Boolean {

        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as StreamDataTypes

        if (!fields.contentEquals(other.fields)) return false
        if (!types.contentEquals(other.types)) return false

        return true
    }

    override fun hashCode(): Int {
        var result = fields.contentHashCode()
        result = 31 * result + types.contentHashCode()
        return result
    }

    fun toTypeInformation(): TypeInformation<Row> {
        return Types.ROW_NAMED(
            fields, *types
        )
    }

    fun fieldType(fieldName: String): TypeInformation<*>? {
        val index = fields.indexOf(fieldName)
        return if (index == -1) {
            null
        } else {
            types[index]
        }
    }

    fun plus(other: StreamDataTypes): StreamDataTypes {
        val allFields = mutableListOf(*fields)
        val allTypes = mutableListOf(*types)

        for (i in 1..other.fields.size) {
            if (!allFields.contains(other.fields[i])) {
                allFields.add(other.fields[i])
                allTypes.add(other.types[i])
            }
        }

        return StreamDataTypes(allFields.toTypedArray(), allTypes.toTypedArray())
    }

    fun minus(other: StreamDataTypes): StreamDataTypes {
        return drop(other.fields)
    }

    fun rename(fieldMap: Map<String, String>): StreamDataTypes {
        val allFields = mutableListOf(*fields)

        for ((fromName, toName) in fieldMap) {
            if (fromName == toName) {
                continue
            }

            val index = allFields.indexOf(fromName)
            if (index != -1 && index >=0  && index < allFields.size) {
                allFields[index] = toName
            }
        }

        return StreamDataTypes(allFields.toTypedArray(), types)
    }

    fun select(output: Array<String>): StreamDataTypes {
        val outputTypes = mutableListOf<TypeInformation<*>>()
        for (i in 1..output.size) {
            val fieldIndex  = fields.indexOf(output[i])
            if (fieldIndex != -1) {
                outputTypes.add(types[fieldIndex])
            }
        }

        return StreamDataTypes(output, outputTypes.toTypedArray())
    }

    fun drop(dropFields: Array<String>): StreamDataTypes {
        val allFields = mutableListOf(*fields)
        val allTypes = mutableListOf(*types)

        for (i in 1..dropFields.size) {
            val existIndex =allFields.indexOf(dropFields[i])
            if (existIndex != -1) {
                allFields.removeAt(existIndex)
                allTypes.removeAt(existIndex)
            }
        }

        return StreamDataTypes(allFields.toTypedArray(), allTypes.toTypedArray())
    }

    override fun toString(): String {
        val size = min(fields.size, types.size)
        val results = mutableListOf<String>()
        for (i in 0 until size) {
            results.add("${fields[i]}[${types[i]}]")
        }

        return "Types(${results.joinToString(separator = " , ")})"
    }

    companion object {
        val INIT = StreamDataTypes(emptyArray(), emptyArray())
        private val typeMap = mapOf(
            SQLTypes.BIT to Types.INT,
            SQLTypes.BOOLEAN to Types.BOOLEAN,
            SQLTypes.CHAR to Types.STRING,
            SQLTypes.VARCHAR to Types.STRING,
            SQLTypes.NCHAR to Types.STRING,
            SQLTypes.NVARCHAR to Types.STRING,
            SQLTypes.LONGNVARCHAR to Types.STRING,
            SQLTypes.LONGVARCHAR to Types.STRING,
            SQLTypes.TIME to Types.LOCAL_TIME,
            SQLTypes.DATE to Types.LOCAL_DATE,
            SQLTypes.TIMESTAMP to Types.LOCAL_DATE_TIME,
            SQLTypes.INTEGER to Types.INT,
            SQLTypes.SMALLINT to Types.SHORT,
            SQLTypes.TINYINT to Types.SHORT,
            SQLTypes.BIGINT to Types.LONG,
            SQLTypes.FLOAT to Types.FLOAT,
            SQLTypes.REAL to Types.FLOAT,
            SQLTypes.DOUBLE to Types.DOUBLE,
            SQLTypes.DECIMAL to Types.BIG_DEC,
            SQLTypes.NUMERIC to Types.BIG_DEC
        )

        @JvmStatic
        fun of(fields: Array<String>, typeInfos: Array<TypeInformation<*>>): StreamDataTypes {
            return StreamDataTypes(fields, typeInfos)
        }

        @JvmStatic
        fun of(fields: Array<String>, dataTypes: Array<DataType>): StreamDataTypes {
            return of (fields, dataTypes.map { TypeInfoDataTypeConverter.fromDataTypeToTypeInfo(it) }.toTypedArray())
        }

        @JvmStatic
        fun of(fields: Array<String>, dataTypes: Array<Int>): StreamDataTypes {
            return of (fields, dataTypes.map { typeMap[it]!! }.toTypedArray())
        }
    }
}