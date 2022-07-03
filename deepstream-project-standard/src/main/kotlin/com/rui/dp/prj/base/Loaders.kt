package com.rui.dp.prj.base

import org.dom4j.Element
import org.dom4j.io.SAXReader

object PackageResourceJobDataLoader: JobDataLoader() {
    override fun loadResources(resourceName: String): Element {
        val inputStream = javaClass.getResourceAsStream("/$resourceName.xml")!!
        val reader = SAXReader()
        val document = reader.read(inputStream)
        return document.rootElement
    }

    @JvmStatic
    fun load(): DeepStreamJobData {
        val jobData = loadJobData("job")
        val sqls = loadSql("sqls")
        jobData.setSQLs(sqls)

        val dataSource = loadDatasource("data_source")
        jobData.setDataSourceConfigs(dataSource)

        return jobData
    }
}