package com.rui.ds.ks.delay

import com.google.common.base.Strings
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.*
import java.util.concurrent.Callable
import java.util.concurrent.atomic.AtomicBoolean

class DelayRecordDispatcher(
    bootStrapServers: String,
    private val delayLevel: DelayLevel
) : Runnable {
    private val consumer: KafkaConsumer<String, String>
    private val producer: KafkaProducer<String, DelayRetryRecord>
    val onWorking: AtomicBoolean = AtomicBoolean(false)
    private val delayTopic: String

    init {
        val props = Properties()
        props["bootstrap.servers"] = bootStrapServers
        props["group.id"] = DEFAULT_GROUP_ID
        props["key.deserializer"] = "org.apache.kafka.common.serialization.StringDeserializer"
        props["value.deserializer"] = "org.apache.kafka.common.serialization.StringDeserializer"
        props["enable.auto.commit"] = "false"

        consumer = KafkaConsumer<String, String>(props)

        val productProps = Properties()
        productProps["bootstrap.servers"] = bootStrapServers
        productProps["key.serializer"] = "org.apache.kafka.common.serialization.StringSerializer"
        productProps["value.serializer"] = "com.rui.ds.ks.delay.DelayEventSerializer"
        this.producer = KafkaProducer(productProps)

        this.delayTopic = DeepStreamDelay.delayTopic(delayLevel)

        consumer.subscribe(listOf(delayTopic))
    }

    override fun run() {
        while (onWorking.get()) {
            val records: ConsumerRecords<String, String> = consumer.poll(Duration.ofMillis(1000))
            for (record in records) {
                while (true) {
                    val sleep: Long = record.timestamp() + delayLevel.delayMinute * 60 * 1000 - System.currentTimeMillis()
                    if (sleep > 0) {
                        try {
                            Thread.sleep(sleep)
                        } catch (e: InterruptedException) {
                            logger.error(e.message)
                        }

                        continue
                    }

                    val delayRecord = DeepStreamDelay.readDelayData(record.value())
                    logger.info("Process delay data {}", delayRecord);

                    if (Strings.isNullOrEmpty(delayRecord.eventTopic)) {
                        logger.warn("unable to read topic from value:{}", record.value())
                        break
                    }

                    val producerRecord = ProducerRecord<String, DelayRetryRecord>(delayTopic, delayRecord)

                    producer.send(producerRecord)
                    logger.debug("send {} to {}", record.value(), delayTopic)

                    break
                }
            }

            consumer.commitSync()
        }
    }

    companion object {
        val logger: Logger = LoggerFactory.getLogger(DelayRecordDispatcher::class.java)

        const val DEFAULT_GROUP_ID: String = "delay_proc"
    }

}