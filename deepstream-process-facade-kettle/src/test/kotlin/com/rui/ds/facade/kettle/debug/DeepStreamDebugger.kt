package com.rui.ds.facade.kettle.debug

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
import org.dom4j.DocumentHelper
import org.dom4j.io.OutputFormat
import org.dom4j.io.SAXReader
import org.dom4j.io.XMLWriter
import java.io.ByteArrayOutputStream

open class DeepStreamDebugger {
    protected val SQL: MutableMap<String, String> = mutableMapOf()
    private val tableSqlDoc = DocumentHelper.createDocument()
    private val rootElement = tableSqlDoc.addElement("sqls")

    fun initEnv(): ProcessContext {
        val jobConfig = JobConfig()
        val processContext = DeepStreamJob.initProcessContext(jobConfig)

        // 初始化运行环境, 注册数据源
        loadDatasource()

        loadSql()

        return processContext
    }

    private fun loadDatasource() {
        val inputStream = javaClass.getResourceAsStream("/debug/data_source.xml")!!

        // read from xml
        val reader = SAXReader()
        val document = reader.read(inputStream)
        val rootElement = document.rootElement
        val connections = rootElement.elements("connection")

        val configs = connections.map {
            KettleJobParser.parseConnection(it)
        }

        configs.forEach {
            DatabaseSources.registryDataSource(it)
        }
    }

    private fun loadSql() {
        val inputStream = javaClass.getResourceAsStream("/debug/debug_sqls.xml")!!
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

    fun createTable(process: ProcessContext, dsName: String, tableContext: TableContext): TableResult {
        val tableEnv = process.tableEnv

        val sql = TableGenerator.getGenerator(dsName, tableContext.tableType).createTableSQL(
            dbName = tableContext.catalog,
            tableName = tableContext.tableName
        )

        val sqlElement = rootElement.addElement("sql")
        sqlElement.addAttribute("name", tableContext.tableName)
        sqlElement.addCDATA("\n$sql\n")

        return tableEnv.executeSql(
            sql
        )
    }

    fun outputSqlDocument(): String {
        val format: OutputFormat = OutputFormat.createPrettyPrint()
        val outputStream = ByteArrayOutputStream()
        val writer = XMLWriter(outputStream, format)
        writer.write(tableSqlDoc)

        return String(outputStream.toByteArray())
    }

    fun executeQuery(context: ProcessContext, sql: String): Table {
        return context.tableEnv.sqlQuery(sql)
    }

    fun executeSQL(context: ProcessContext, sql: String): TableResult {
        return context.tableEnv.executeSql(sql)
    }

    fun dataByTable(schema: ResolvedSchema): StreamDataTypes {
        val columns = schema.columns
        val types = StreamDataTypes.of(
            columns.map { it.name }.toTypedArray(),
            columns.map { it.dataType }.toTypedArray()
        )

        return types
    }
}