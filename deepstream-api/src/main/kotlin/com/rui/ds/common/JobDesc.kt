package com.rui.ds.common

import com.google.common.collect.ImmutableTable
import com.google.common.collect.Table

data class Hop(
    val fromStep: String, val toStep: String, val enabled: Boolean = true
)

class DataSourceConfig(
    val name: String,
    val dbName: String,
    val username: String,
    val password: String,
    val type: String,
    val host: String,
    val port: Int = 0,
    val properties: Map<String, String> = mapOf(),
    val subType: Int = DATABASE_SUBTYPE_DEFAULT
) {
    val driverClassName: String = driverMap.getOrDefault(type, "")
    val connectString: String = String.format(connectURLFormat.get(type, subType)!!, host, port, dbName)

    val maximumPoolSize: Int = DEFAULT_MAX_POOL_SIZE
    val minIdle: Int = DEFAULT_MIN_IDLE
    var connectionTimeout: Long = DEFAULT_CONNECTION_TIMEOUT
        private set
    val idleTimeout: Long = DEFAULT_IDLE_TIMEOUT
    val maxLifetime: Long = DEFAULT_MAX_LIFETIME
    val isAutoCommit = false
    var isReadOnly: Boolean = DEFAULT_READ_ONLY

    companion object {
        const val DS_TYPE_ORACLE = "oracle"
        const val DS_TYPE_MYSQL = "mysql"
        const val DS_TYPE_MSSQL = "mssql"
        private const val DEFAULT_CONNECTION_TIMEOUT: Long = 30 * 1000
        private const val DEFAULT_IDLE_TIMEOUT: Long = 10 * 60 * 1000
        private const val DEFAULT_MAX_LIFETIME: Long = 30 * 60 * 1000
        private const val DEFAULT_MAX_POOL_SIZE = 10
        private const val DEFAULT_MIN_IDLE = 1
        private const val DEFAULT_READ_ONLY = false
        public const val DATABASE_SUBTYPE_DEFAULT = 0
        public const val DATABASE_SUBTYPE_CLUSTER = 1
        private val connectURLFormat: Table<String, Int, String> = ImmutableTable.Builder<String, Int, String>().put(
                DS_TYPE_ORACLE, DATABASE_SUBTYPE_DEFAULT, "jdbc:oracle:thin:@%s:%d:%s"
            ).put(
                DS_TYPE_ORACLE, DATABASE_SUBTYPE_CLUSTER, "jdbc:oracle:thin:@%s:%d%s"
            ).put(
                DS_TYPE_MYSQL, DATABASE_SUBTYPE_DEFAULT, "jdbc:mysql://%s:%d/%s?useSSL=false&autoReconnect=true"
            ).put(
                DS_TYPE_MSSQL, DATABASE_SUBTYPE_DEFAULT, "jdbc:sqlserver://%s\\MSSQLSERVER2008:%d;database=%s"
            ).build()
        private val driverMap: Map<String, String> = mapOf(
            DS_TYPE_ORACLE to "oracle.jdbc.driver.OracleDriver",
            DS_TYPE_MYSQL to "com.mysql.cj.jdbc.Driver",
            DS_TYPE_MSSQL to "com.microsoft.sqlserver.jdbc.SQLServerDriver"
        )
    }

    override fun toString(): String {
        return "DataSourceConfig(name='$name', dbName='$dbName', url='$connectString')"
    }
}
