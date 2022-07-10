package com.rui.ds.ks.delay

import com.google.common.cache.*
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import java.time.Duration
import java.util.*

class DelayRecordPublisher private constructor(
    bootStrapServers: String,
    delayLevel: DelayLevel
) {
    private val producer: KafkaProducer<String, String>
    private val delayTopic: String

    init {
        val productProps = Properties()
        productProps["bootstrap.servers"] = bootStrapServers
        productProps["key.serializer"] = "org.apache.kafka.common.serialization.StringSerializer"
        productProps["value.serializer"] = "org.apache.kafka.common.serialization.StringSerializer"
        this.producer = KafkaProducer(productProps)

        this.delayTopic = DeepStreamDelay.delayTopic(delayLevel)
    }

    fun publishDelayRecord(record: DelayRetryRecord) {
        val recordMsg = DeepStreamDelay.serializeDelayRecord(record)
        val producerRecord = ProducerRecord<String, String>(delayTopic, recordMsg)

        producer.send(producerRecord)
    }

    fun close() {
        producer.close()
    }

    companion object {
        private val publishers: LoadingCache<DelayServiceKey, DelayRecordPublisher> =
            CacheBuilder.newBuilder()
                .removalListener(
                    RemovalListener<DelayServiceKey, DelayRecordPublisher> { notification -> notification.value.close() }
                )
                .expireAfterAccess(Duration.ofMinutes(60))
                .build(
                    object : CacheLoader<DelayServiceKey, DelayRecordPublisher>() {
                        override fun load(key: DelayServiceKey): DelayRecordPublisher {
                            return DelayRecordPublisher(key.servers, key.level)
                        }
                    }
                )

        @JvmStatic
        fun publish(servers: String, level: DelayLevel, record: DelayRetryRecord) {
            publishers.get(DelayServiceKey(servers, level)).publishDelayRecord(record)
        }
    }

}