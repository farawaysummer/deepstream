package com.rui.dp.prj.base

import com.rui.ds.datasource.DatabaseSources
import com.rui.ds.steps.transform.DictMappingFunction

object DeepStreamFunctions {

    @JvmStatic
    fun createAsyncJdbcFunction(businessData: BusinessData): AsyncDBJoinFunction {
        return AsyncDBJoinFunction(businessData)
    }

    @JvmStatic
    fun createDictMappingFunction(jobName: String, columns: Array<String>): DictMappingFunction {
        // get job id from name
        var jobId = -1L
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
    fun createDictMappingFunction(jobId: Long, columns: Array<String>): DictMappingFunction {
        return DictMappingFunction(jobId, columns)
    }
}