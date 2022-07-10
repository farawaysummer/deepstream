package com.rui.ds.ks.delay

import com.fasterxml.jackson.databind.ObjectMapper

object DeepStreamDelay {
    private const val DELAY_TOPIC: String = "dp-delay"
    const val FIELD_DEAD_LINE = "deadline"

    private val mapper: ObjectMapper = ObjectMapper()

    @JvmStatic
    fun serializeDelayRecord(record: DelayRetryRecord): String {
        return mapper.writeValueAsString(record)
    }

    @JvmStatic
    fun readDelayData(message: String): DelayRetryRecord {
        // parse from json string to recordO
        return mapper.readValue(message, DelayRetryRecord::class.java)
    }

    @JvmStatic
    fun delayTopic(level: DelayLevel): String {
        return "$DELAY_TOPIC-${level.delayMinute}"
    }
}