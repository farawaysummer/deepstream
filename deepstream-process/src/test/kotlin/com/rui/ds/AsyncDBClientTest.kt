package com.rui.ds

import com.github.jasync.sql.db.Configuration
import com.github.jasync.sql.db.Connection
import com.github.jasync.sql.db.ConnectionPoolConfiguration
import com.github.jasync.sql.db.general.ArrayRowData
import com.github.jasync.sql.db.mysql.pool.MySQLConnectionFactory
import com.github.jasync.sql.db.pool.ConnectionPool
import org.junit.Test

internal class AsyncDBClientTest {

    @Test
    fun `check async db client query`() {
        val poolConfiguration = ConnectionPoolConfiguration(
            maxActiveConnections = 10,
            maxIdleTime = 5 * 1000 * 60,
            maxPendingQueries = 100,
            connectionValidationInterval = 30 * 1000
        )

        val connection: Connection = ConnectionPool(
            MySQLConnectionFactory(
                Configuration(
                    username = "reseig",
                    password = "mu-xPL7C",
                    host = "192.168.3.168",
                    port = 3216,
                    database = "test"
                )
            ),
            poolConfiguration
        )
        connection.connect().get()

        val future = connection.sendQuery("select * from f_result")
        val queryResult = future.get()

        for (row in queryResult.rows) {
            println((row as ArrayRowData).columns.toList())
            println(row.mapping)
            break
        }

    }

}