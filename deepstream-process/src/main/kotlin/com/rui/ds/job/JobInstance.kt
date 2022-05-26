package com.rui.ds.job

class JobInstance(
    private val job: DeepStreamJob
) {

    fun execute(config: JobConfig) {
        val processContext = DeepStreamJob.initProcessContext(config)

        job.visit(processContext)
    }

}