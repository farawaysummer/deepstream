package com.rui.dp.prj.base

import com.rui.dp.prj.base.job.ProcessJobData
import com.rui.dp.prj.base.job.SyncJobData
import com.rui.dp.prj.base.job.ProcessJobDataLoader
import com.rui.dp.prj.base.job.SyncJobDataLoader
import org.dom4j.Element
import org.dom4j.io.SAXReader
import java.io.File

object PackageResourceJobDataLoader : ProcessJobDataLoader() {
    override fun loadResources(resourceName: String): Element {
        val inputStream = javaClass.getResourceAsStream("/$resourceName.xml")!!
        val reader = SAXReader()
        val document = reader.read(inputStream)
        logger.info("Loading resource $resourceName :\n ${document.asXML()}")
        return document.rootElement
    }

    @JvmStatic
    fun load(): ProcessJobData {
        val jobData = loadJobData("job")
        val sqls = loadSql("sqls")
        jobData.setSQLs(sqls)

        val dataSource = loadDatasource("data_source")
        jobData.setDataSourceConfigs(dataSource)

        return jobData
    }
}

class ExternalResourceJobDataLoader(
    private val resourcePath: String
) : ProcessJobDataLoader() {

    override fun loadResources(resourceName: String): Element {
        val filePath =
            if (resourcePath.endsWith("/")) {
                "${resourcePath}${resourceName}.xml"
            } else {
                "${resourcePath}/${resourceName}.xml"
            }

        val resourceFile = File(filePath)
        if (!resourceFile.exists()) {
            throw RuntimeException("Can't find resource path $resourcePath")
        }

        val reader = SAXReader()
        val document = reader.read(resourceFile)

        logger.info("Loading resource ${resourceFile.absolutePath} :\n ${document.asXML()}")

        return document.rootElement
    }

    fun load(): ProcessJobData {
        val jobData = loadJobData("job")
        val sqls = loadSql("sqls")
        jobData.setSQLs(sqls)

        val dataSource = loadDatasource("data_source")
        jobData.setDataSourceConfigs(dataSource)

        return jobData
    }
}

object PackageResourceSyncJobDataLoader : SyncJobDataLoader() {
    override fun loadResources(resourceName: String): Element {
        val inputStream = javaClass.getResourceAsStream("/$resourceName.xml")!!
        val reader = SAXReader()
        val document = reader.read(inputStream)
        logger.info("Loading resource $resourceName :\n ${document.asXML()}")
        return document.rootElement
    }

    @JvmStatic
    fun load(): SyncJobData {
        val jobData = loadJobData("job")
        val sqls = loadSql("sqls")
        jobData.setSQLs(sqls)

        return jobData
    }
}

class ExternalResourceSyncJobDataLoader(
    private val resourcePath: String
) : SyncJobDataLoader() {

    override fun loadResources(resourceName: String): Element {
        val filePath =
            if (resourcePath.endsWith("/")) {
                "${resourcePath}${resourceName}.xml"
            } else {
                "${resourcePath}/${resourceName}.xml"
            }

        val resourceFile = File(filePath)
        if (!resourceFile.exists()) {
            throw RuntimeException("Can't find resource path $resourcePath")
        }

        val reader = SAXReader()
        val document = reader.read(resourceFile)
        logger.info("Loading resource ${resourceFile.absolutePath} :\n ${document.asXML()}")
        return document.rootElement
    }

    fun load(): SyncJobData {
        val jobData = loadJobData("job")
        val sqls = loadSql("sqls")
        jobData.setSQLs(sqls)

        return jobData
    }
}