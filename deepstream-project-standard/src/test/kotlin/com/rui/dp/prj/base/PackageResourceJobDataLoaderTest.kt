package com.rui.dp.prj.base

import org.junit.Assert.*
import org.junit.Test

class PackageResourceJobDataLoaderTest {

    @Test
    fun `test job data loader`() {
        val jobData = PackageResourceJobDataLoader.load()
        println(jobData)
    }

}