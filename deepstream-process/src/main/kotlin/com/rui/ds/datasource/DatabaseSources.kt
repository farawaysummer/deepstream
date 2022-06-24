package com.rui.ds.datasource

import com.github.jasync.sql.db.Configuration
import com.github.jasync.sql.db.ConnectionPoolConfiguration
import com.github.jasync.sql.db.mysql.pool.MySQLConnectionFactory
import com.github.jasync.sql.db.pool.ConnectionPool
import com.google.common.cache.CacheBuilder
import com.google.common.cache.CacheLoader
import com.google.common.cache.LoadingCache
import com.rui.ds.common.DataSourceConfig
import com.rui.ds.log.Logging
import com.rui.ds.log.logger
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import java.sql.Connection
import javax.sql.DataSource

typealias AsyncConnection = com.github.jasync.sql.db.Connection

object DatabaseSources : Logging {
    // TODO 修改为LoadingCache方式
    private val dataSourceConfigs: MutableMap<String, DataSourceConfig> = mutableMapOf()

    private val poolConfiguration = ConnectionPoolConfiguration(
        maxActiveConnections = 10,
        maxIdleTime = 5 * 1000 * 60,
        maxPendingQueries = 100,
        connectionValidationInterval = 30 * 1000
    )

    private val dataSourceCache: LoadingCache<String, HikariDataSource>
    private val asyncConnCache: LoadingCache<String, AsyncConnection>

    init {
        dataSourceCache = CacheBuilder.newBuilder().build(
            object : CacheLoader<String, HikariDataSource>() {
                override fun load(dsName: String): HikariDataSource {
                    val config = getDataSourceConfig(dsName) ?: throw Exception("No data source config $dsName found.")
                    val hikariConfig = HikariConfig()

                    logger().info("加载数据源$config")

                    hikariConfig.jdbcUrl = config.connectString
                    hikariConfig.username = config.username
                    hikariConfig.password = config.password
                    hikariConfig.isAutoCommit = false
                    hikariConfig.isReadOnly = config.isReadOnly
                    hikariConfig.maximumPoolSize = config.maximumPoolSize
                    hikariConfig.minimumIdle = config.minIdle
                    hikariConfig.connectionTimeout = config.connectionTimeout
                    hikariConfig.idleTimeout = config.idleTimeout
                    hikariConfig.maxLifetime = config.maxLifetime
                    hikariConfig.driverClassName = config.driverClassName

                    hikariConfig.transactionIsolation = "TRANSACTION_READ_COMMITTED"
                    if (config.type == DataSourceConfig.DS_TYPE_ORACLE) {
                        hikariConfig.connectionTestQuery = "SELECT 1 FROM DUAL"
                    } else {
                        hikariConfig.connectionTestQuery = "SELECT 1"
                    }

                    return HikariDataSource(hikariConfig)
                }
            }
        )

        asyncConnCache = CacheBuilder.newBuilder().build(
            object : CacheLoader<String, AsyncConnection>() {
                override fun load(dsName: String): AsyncConnection {
                    val conf = getDataSourceConfig(dsName) ?: throw Exception("No data source config $dsName found.")
                    val connection: AsyncConnection = ConnectionPool(
                        MySQLConnectionFactory(
                            Configuration(
                                username = conf.username,
                                password = conf.password,
                                host = conf.host,
                                port = conf.port,
                                database = conf.dbName
                            )
                        ),
                        poolConfiguration
                    )
                    connection.connect().get()

                    return connection
                }
            }
        )
    }

    @JvmStatic
    fun registryDataSource(config: DataSourceConfig) {
        dataSourceConfigs[config.name] = config
    }

    @JvmStatic
    fun getDataSourceConfig(name: String): DataSourceConfig? {
        return dataSourceConfigs[name]
    }

    @JvmStatic
    fun getDataSource(name: String): DataSource? {
        return dataSourceCache.get(name)
    }

    @JvmStatic
    fun getConnection(name: String): Connection? {
        return dataSourceCache[name].connection
    }

    @JvmStatic
    fun getAsyncConnection(name: String): AsyncConnection {
        return asyncConnCache.get(name)
    }
}