package com.rui.dp.prj.base

import org.junit.Assert.*
import org.junit.Test

class PackageResourceJobDataLoaderTest {

    @Test
    fun `test job data loader`() {
        val jobData = PackageResourceJobDataLoader.load()

        println(jobData.processData.useDictMapping())

        println(jobData)
//        println("========CREATE EVENT TABLE=========")
//        println(jobData.eventData.toEventTableSql())
//
//        println("\n========QUERY EVENT TABLE=========")
//        println(jobData.eventData.toEventQuerySql())

//        assert(jobData.relatedTables.isNotEmpty())
//
//        println("\n========CREATE RELATED TABLE=========")
//        jobData.relatedTables.forEach {
//            println("========TABLE [${it.tableName}]=========")
//            println(it.toTableSql())
//        }


    }

}