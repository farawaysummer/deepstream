package com.rui.dp.prj.base

object KafkaConsumeTest {

    @JvmStatic
    fun main(args: Array<String>) {

        val context = DeepStreamHelper.initEnv()
        val kafkaSql = """
            CREATE TABLE KTEST (
                `businessId` STRING,
                `futureTime` TIMESTAMP(3),
                `rowData` STRING
            ) WITH (
            'connector' = 'kafka',
            'topic' = 'ktest',
            'properties.bootstrap.servers' = '192.168.4.207:9092',
            'properties.group.id' = 'testGroup',
            'scan.startup.mode' = 'earliest-offset',
            'value.format' = 'json'
            )
            
            
        """.trimIndent()

        DeepStreamHelper.executeSQL(context, kafkaSql)

        val queryOnTime = """
            select *, LOCALTIMESTAMP from KTEST where `futureTime` < LOCALTIMESTAMP
        """.trimIndent()

        DeepStreamHelper.executeSQL(context, queryOnTime).print()
    }

}