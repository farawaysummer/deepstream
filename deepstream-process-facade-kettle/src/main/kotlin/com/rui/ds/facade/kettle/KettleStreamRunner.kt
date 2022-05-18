package com.rui.ds.facade.kettle

import com.google.common.io.Files
import com.rui.ds.job.JobConfig
import com.rui.ds.job.JobInstance
import java.io.File
import java.nio.charset.Charset

object KettleStreamRunner {
    private val parser = KettleJobParser()

    fun executeKtr(ktrFile: String) {
        val content = Files.asCharSource(File(ktrFile), Charset.forName("GBK")).read()
        val job = parser.parse(content)
        val instance = JobInstance(job)

        val config = JobConfig()
        instance.execute(config)
    }
}