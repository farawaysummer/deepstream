package com.rui.ds.facade.kettle.meta

typealias SQLTypes = java.sql.Types

object KettleTypes {

    /** Value type indicating that the value has no type set  */
    const val TYPE_NONE = 0

    /** Value type indicating that the value contains a floating point double precision number.  */
    const val TYPE_NUMBER = 1

    /** Value type indicating that the value contains a text String.  */
    const val TYPE_STRING = 2

    /** Value type indicating that the value contains a Date.  */
    const val TYPE_DATE = 3

    /** Value type indicating that the value contains a boolean.  */
    const val TYPE_BOOLEAN = 4

    /** Value type indicating that the value contains a long integer.  */
    const val TYPE_INTEGER = 5

    /** Value type indicating that the value contains a floating point precision number with arbitrary precision.  */
    const val TYPE_BIGNUMBER = 6

    /** Value type indicating that the value contains an Object.  */
    const val TYPE_SERIALIZABLE = 7

    /** Value type indicating that the value contains binary data: BLOB, CLOB, ...  */
    const val TYPE_BINARY = 8

    /** Value type indicating that the value contains a date-time with nanosecond precision  */
    const val TYPE_TIMESTAMP = 9

    /** Value type indicating that the value contains a Internet address  */
    const val TYPE_INET = 10

    /** The Constant typeCodes.  */
    val typeCodes = arrayOf(
        "-", "Number", "String", "Date", "Boolean", "Integer", "BigNumber", "Serializable", "Binary", "Timestamp",
        "Internet Address"
    )

    val typeMap: Map<String, Int> = mapOf(
        "-" to SQLTypes.NULL,
        "Number" to SQLTypes.NUMERIC,
        "String" to SQLTypes.VARCHAR,
        "Date" to SQLTypes.DATE,
        "Boolean" to SQLTypes.BOOLEAN,
        "Integer" to SQLTypes.INTEGER,
        "BigNumber" to SQLTypes.NUMERIC,
        "Serializable" to SQLTypes.VARBINARY,
        "Binary" to SQLTypes.BINARY,
        "Timestamp" to SQLTypes.TIMESTAMP,
        "Internet Address" to SQLTypes.VARCHAR
    )
}