package com.rui.dp.prj.base

import com.rui.ds.common.DataSourceConfig
import com.rui.ds.datasource.DatabaseSources
import com.rui.ds.steps.transform.DictMappingFunction
import com.rui.ds.steps.transform.dm.DPTransformGateway
import java.util.*

object DeepStreamFunctions {

    @JvmStatic
    fun createAsyncJdbcFunction(businessData: BusinessData): AsyncDBJoinFunction {
        return AsyncDBJoinFunction(businessData)
    }

    @JvmStatic
    fun createAsyncJdbcFunction(businessData: BusinessData, delayQuerySecond: Int): AsyncDBJoinFunction {
        return AsyncDBJoinFunction(businessData, delayQuerySecond)
    }

    fun createValueMappingFunctions(jobs: List<String>, columns: Array<String>): ValueMappingFunction {
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

        return ValueMappingFunction(jobIds, columns)
    }

    @JvmStatic
    fun createDictMappingFunction(jobName: String, columns: Array<String>): DictMappingFunction {
        // get job id from name
        var jobId = -1L
        val mcDsConfig = DatabaseSources.getDataSourceConfig("MC_DS")
        if (mcDsConfig == null) {
            loadMCDataSource()
        }

        val connection = DatabaseSources.getConnection("MC_DS")!!
        connection.use {
            val statement = connection.createStatement()
            val resultSet = statement.executeQuery("select id from eigmcdb.realesoft_transform_job where name='$jobName'")
            if (resultSet.next()) {
                jobId = resultSet.getLong(1)
            }
        }

        if (jobId == -1L) {
            throw RuntimeException("Can't find transform job with name $jobName")
        }

        return DictMappingFunction(jobId, columns)
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

    @JvmStatic
    fun createDictMappingFunction(jobId: Long, columns: Array<String>): DictMappingFunction {
        return DictMappingFunction(jobId, columns)
    }
}