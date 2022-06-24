package com.rui.dp.prj.job

import com.rui.ds.ProcessContext
import com.rui.ds.StreamDataTypes
import com.rui.ds.common.TableContext
import com.rui.ds.datasource.DatabaseSources
import com.rui.ds.facade.kettle.KettleJobParser
import com.rui.ds.generator.TableGenerator
import com.rui.ds.job.DeepStreamJob
import com.rui.ds.job.JobConfig
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.TableResult
import org.apache.flink.table.catalog.ResolvedSchema
import org.dom4j.io.SAXReader

object DeepStreamHelper {
    val SQL: MutableMap<String, String> = mutableMapOf()

    @JvmStatic
    fun initEnv(): ProcessContext {
        loadDatasource()

        loadSql()

        val jobConfig = JobConfig(miniBatchEnabled = false)

        val processContext = DeepStreamJob.initProcessContext(jobConfig)

        // 初始化运行环境, 注册数据源


        return processContext
    }

    @JvmStatic
    fun loadDatasource() {
        val inputStream = javaClass.getResourceAsStream("/data_source.xml")!!

        // read from xml
        val reader = SAXReader()
        val document = reader.read(inputStream)
        val rootElement = document.rootElement

        val connections = rootElement.elements("connection")

        val configs = connections.map {
            println("parse $it")
            KettleJobParser.parseConnection(it)
        }

        configs.forEach {
            println("registey $it")
            DatabaseSources.registryDataSource(it)
        }
    }

    @JvmStatic
    fun loadSql() {
        val inputStream = javaClass.getResourceAsStream("/sqls.xml")!!
        val reader = SAXReader()
        val document = reader.read(inputStream)
        val rootElement = document.rootElement
        val sqlElements = rootElement.elements("sql")
        sqlElements.forEach {
            val name = it.attributeValue("name")
            val sql = it.text
            SQL[name] = sql
        }
    }

    @JvmStatic
    fun createTable(process: ProcessContext, dsName: String, tableContext: TableContext): TableResult {
        val tableEnv = process.tableEnv

        return tableEnv.executeSql(
            TableGenerator.getGenerator(dsName, tableContext.tableType).createTableSQL(
                dbName = tableContext.catalog,
                tableName = tableContext.tableName
            )
        )
    }

    @JvmStatic
    fun getSql(name: String): String? {
        return SQL[name]
    }

    @JvmStatic
    fun executeQuery(context: ProcessContext, sql: String): Table {
        return context.tableEnv.sqlQuery(sql)
    }

    @JvmStatic
    fun executeSQL(context: ProcessContext, sql: String): TableResult {
        return context.tableEnv.executeSql(sql)
    }

    @JvmStatic
    fun dataByTable(schema: ResolvedSchema): StreamDataTypes {
        val columns = schema.columns
        val types = StreamDataTypes.of(
            columns.map { it.name }.toTypedArray(),
            columns.map { it.dataType }.toTypedArray()
        )

        return types
    }
}