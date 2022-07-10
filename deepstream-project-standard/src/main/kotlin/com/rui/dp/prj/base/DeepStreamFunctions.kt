package com.rui.dp.prj.base

import com.rui.dp.prj.base.funs.AsyncDBJoinFunction
import com.rui.dp.prj.base.funs.ValueMappingFunction
import com.rui.dp.prj.base.job.ProcessJobData
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
        jobData: ProcessJobData,
        jobs: List<String>,
        columns: List<String>
    ): ValueMappingFunction {
//        val mcDsConfig = jobData.getDataSourceConfig("MC_DS")

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

}