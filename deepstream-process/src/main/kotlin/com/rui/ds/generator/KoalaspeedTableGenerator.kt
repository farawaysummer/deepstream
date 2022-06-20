package com.rui.ds.generator

import com.rui.ds.common.DataSourceConfig

class KoalaspeedTableGenerator (
    dsConfig: DataSourceConfig,
    tablePrefix: String = ""
) : KafkaTableGenerator(dsConfig, tablePrefix) {

    override fun createConnectorInfo(dbName: String, tableName: String): Map<String, Any> {
        return mapOf(
            "topic" to "t_${tableName.lowercase()}",
            "properties.bootstrap.servers" to dsConfig.properties["kafka.server"]!!,
            "properties.group.id" to dsConfig.properties.getOrDefault("kafka.groupid", "k_default"),
            "scan.startup.mode" to dsConfig.properties.getOrDefault("kafka.scan.mode", "group-offsets"),
            "format" to "ruisoft-koalaspeed"
        )
    }
}