package com.rui.ds.job

class JobInstance(
    private val job: DeepStreamJob
) {

    fun execute(config: JobConfig) {
        val processContext = job.initDebugContext(config)

        job.visit(processContext)
    }

}