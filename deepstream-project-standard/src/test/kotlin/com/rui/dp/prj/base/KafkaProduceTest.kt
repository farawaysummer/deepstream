package com.rui.dp.prj.base

object KafkaProduceTest {

    @JvmStatic
    fun main(args: Array<String>) {
        val dataGenSql = """
            CREATE TABLE MockTable (
                `businessId` STRING,
                `futureTime` TIMESTAMP(3),
                `rowData` STRING
            ) WITH (
              'connector' = 'datagen',
              'rows-per-second' = '1'
            )

        """.trimIndent()

        val context = DeepStreamHelper.initEnv()

        DeepStreamHelper.executeSQL(context, dataGenSql)

        val kafkaSql = """
            CREATE TABLE KTEST (
                `businessId` STRING,
                `futureTime` TIMESTAMP(3),
                `rowData` STRING,
                PRIMARY KEY (`businessId`) NOT ENFORCED
            ) WITH (
            'connector' = 'upsert-kafka',
            'topic' = 'ktest',
            'properties.bootstrap.servers' = '192.168.4.207:9092',
            'properties.group.id' = 'testGroup',
            'key.format' = 'json',
            'value.format' = 'json'
            )
            
            
        """.trimIndent()

        DeepStreamHelper.executeSQL(context, kafkaSql)

        val insertSql = """
            insert into KTEST (`businessId`, `futureTime`, `rowData`) 
                select 
                    `businessId`,
                     TIMESTAMPADD(MINUTE,3,`futureTime`),
                     `rowData`
                from MockTable
        """.trimIndent()

        DeepStreamHelper.executeSQL(context, insertSql)

    }
}