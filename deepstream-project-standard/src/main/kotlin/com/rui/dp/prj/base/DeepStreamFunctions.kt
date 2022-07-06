package com.rui.dp.prj.base

import com.rui.dp.prj.base.funs.AsyncDBJoinFunction
import com.rui.dp.prj.base.funs.ValueMappingFunction
import com.rui.dp.prj.base.job.DeepStreamProcessJobData
import com.rui.dp.prj.base.job.QueryData
import com.rui.ds.common.DataSourceConfig
import com.rui.ds.datasource.DatabaseSources
import java.util.*

object DeepStreamFunctions {

    @JvmStatic
    fun createAsyncJdbcFunction(businessData: QueryData): AsyncDBJoinFunction {
        return AsyncDBJoinFunction(businessData)
    }

    @JvmStatic
    fun createAsyncJdbcFunction(businessData: QueryData, delayQuerySecond: Int): AsyncDBJoinFunction {
        return AsyncDBJoinFunction(businessData, delayQuerySecond)
    }

    @JvmStatic
    fun createValueMappingFunctions(
        jobData: DeepStreamProcessJobData,
        jobs: List<String>,
        columns: List<String>
    ): ValueMappingFunction {
        val mcDsConfig = DatabaseSources.getDataSourceConfig("MC_DS")
        if (mcDsConfig == null) {
            loadMCDataSource()
        }

        val connection = DatabaseSources.getConnection("MC_DS")!!
        val jobIds = mutableListOf<Long>()
        connection.use {
            for (jobName in jobs) {
                val statement = connection.createStatement()
                val resultSet =
                    statement.executeQuery("select id from eigmcdb.realesoft_transform_job where name='$jobName'")
                if (resultSet.next()) {
                    jobIds.add(resultSet.getLong(1))
                }
            }
        }

        if (jobIds.isEmpty()) {
            throw RuntimeException("Can't find transform job with name $jobs")
        }

        return ValueMappingFunction(jobIds, columns.toTypedArray(), jobData)
    }

    @JvmStatic
    private fun loadMCDataSource() {
        val resource = DeepStreamFunctions::class.java.getResourceAsStream("/source.properties")!!
        val sourceProp = Properties()
        sourceProp.load(resource)

        val mcDsConf = DataSourceConfig(
            name = "MC_DS",
            dbName = "eigmcdb",
            username = sourceProp.getProperty("eigmc.db.username"),
            password = sourceProp.getProperty("eigmc.db.password"),
            type = "mysql",
            host = sourceProp.getProperty("eigmc.db.hostname"),
            port = sourceProp.getProperty("eigmc.db.port").toInt()
        )

        DatabaseSources.registryDataSource(mcDsConf)
    }
}