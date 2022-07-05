package com.rui.dp.prj.base

import org.dom4j.Element
import org.dom4j.io.SAXReader
import java.io.File

object PackageResourceJobDataLoader : JobDataLoader() {
    override fun loadResources(resourceName: String): Element {
        val inputStream = javaClass.getResourceAsStream("/$resourceName.xml")!!
        val reader = SAXReader()
        val document = reader.read(inputStream)
        return document.rootElement
    }

    @JvmStatic
    fun load(): DeepStreamProcessJobData {
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
) : JobDataLoader() {

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
        return document.rootElement
    }

    fun load(): DeepStreamProcessJobData {
        val jobData = loadJobData("job")
        val sqls = loadSql("sqls")
        jobData.setSQLs(sqls)

        val dataSource = loadDatasource("data_source")
        jobData.setDataSourceConfigs(dataSource)

        return jobData
    }
}