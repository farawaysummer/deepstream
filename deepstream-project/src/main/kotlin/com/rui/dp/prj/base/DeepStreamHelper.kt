package com.rui.dp.prj.base

import com.rui.ds.ProcessContext
import com.rui.ds.StreamDataTypes
import com.rui.ds.common.TableContext
import com.rui.ds.datasource.DatabaseSources
import com.rui.ds.facade.kettle.KettleJobParser
import com.rui.ds.generator.TableGenerator
import com.rui.ds.job.DeepStreamJob
import com.rui.ds.job.JobConfig
import com.rui.ds.types.DeepStreamTypes
import io.debezium.util.Strings
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.TableResult
import org.apache.flink.table.catalog.ResolvedSchema
import org.apache.flink.table.runtime.typeutils.TimestampDataTypeInfo
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

    fun loadBusiness(): BusinessData {
        val inputStream = javaClass.getResourceAsStream("/business.xml")!!
        val reader = SAXReader()
        val document = reader.read(inputStream)
        val rootElement = document.rootElement

        val businessName = rootElement.attributeValue("name")
        val dsName = rootElement.element("datasource").attributeValue("name").trim()
        val sqlName = rootElement.element("businessSql").attributeValue("name").trim()
        val businessSql = getSql(sqlName)!!
        val dictTransform = rootElement.element("transformJobName")?.text

        val relatedTables = rootElement.element("relates").elements("table").map { it.attributeValue("name") }

        val conditionFields =
            rootElement.element("conditions").elements("condition").map { it.attributeValue("name") }.toList()

        val resultFields = rootElement.element("results").elements("field")
            .associateBy({ it.attributeValue("name").trim() }, { it.attributeValue("type").trim() })

        return BusinessData(
            businessName = businessName,
            dsName = dsName,
            relatedTables = relatedTables,
            dictTransformName = dictTransform,
            businessSql = businessSql,
            conditionFields = conditionFields,
            resultFields = resultFields
        )
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
            KettleJobParser.parseConnection(it)
        }

        configs.forEach {
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

    fun toStreamDataTypes(typeMap: Map<String, String>): StreamDataTypes {
        val allTypes = typeMap.mapValues { (_, value) ->
            mappingTypeNameToInformation(value)
        }

        return StreamDataTypes(allTypes.keys.toTypedArray(), allTypes.values.toTypedArray())
    }

    fun mappingTypeNameToInformation(typeName: String): TypeInformation<*> {
        return when (typeName) {
            "VARCHAR" -> Types.STRING
            "VARCHAR2" -> Types.STRING
            "CHAR" -> Types.STRING
            "TIMESTAMP" -> Types.LOCAL_DATE_TIME
            "DATE" -> Types.LOCAL_DATE
            "TIME" -> Types.LOCAL_TIME
            "NUMBER" -> Types.BIG_DEC
            else -> Types.STRING
        } as TypeInformation<*>
    }

    fun mappingTypeNameToNormalize(typeName: String): String {
        return when (typeName) {
            "VARCHAR" -> DeepStreamTypes.DATA_TYPE_STRING
            "VARCHAR2" -> DeepStreamTypes.DATA_TYPE_STRING
            "CHAR" -> DeepStreamTypes.DATA_TYPE_STRING
            "TIMESTAMP" -> DeepStreamTypes.DATA_TYPE_TIMESTAMP
            "DATE" -> DeepStreamTypes.DATA_TYPE_DATE
            "TIME" -> DeepStreamTypes.DATA_TYPE_TIME
            "NUMBER" -> DeepStreamTypes.DATA_TYPE_DECIMAL
            else -> DeepStreamTypes.DATA_TYPE_STRING
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