package com.rui.ds.facade.kettle

import com.google.common.io.Files
import com.rui.ds.job.JobConfig
import com.rui.ds.job.JobInstance
import com.rui.ds.log.Logging
import com.rui.ds.log.logger
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import java.io.File
import java.nio.charset.Charset
import kotlin.io.path.Path
import kotlin.io.path.listDirectoryEntries

object KettleStreamRunner : Logging {

    fun executeKtr(ktrFile: String) {
        logger().debug("开始执行脚本$ktrFile")
        val content = Files.asCharSource(File(ktrFile), Charset.forName("GBK")).read()

        val job = KettleJobParser.parse(content)

        logger().info("转换作业解析内容:$job")

        val instance = JobInstance(job)

        val config = JobConfig()
        instance.execute(config)
    }

    @JvmStatic
    fun main(args: Array<String>) {
        System.setProperty("Log4jContextSelector", "org.apache.logging.log4j.core.async.AsyncLoggerContextSelector")
        val workPath: String = if (args.isEmpty()) {
            "deploy/"
        } else {
            args[0]
        }

        System.setProperty("service.workPath", workPath)
        logger().info("开始处理实时数据脚本.")

        // 获取脚本路径
        val scriptPath = "${workPath}scripts/"
        val scripts = Path(scriptPath).listDirectoryEntries(glob = "*.ktr")

        runBlocking {
            scripts.map {
                launch {
                    executeKtr(it.toString())
                }
            }.joinAll()
        }

        logger().info("处理实时数据脚本完成.")
    }

}