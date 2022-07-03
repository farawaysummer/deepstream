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
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.TableResult
import org.apache.flink.table.catalog.ResolvedSchema
import org.dom4j.io.SAXReader

object DeepStreamHelper {
    @JvmStatic
    fun initEnv(): ProcessContext {
        val jobConfig = JobConfig(miniBatchEnabled = false)
        // 初始化运行环境, 注册数据源
        return DeepStreamJob.initProcessContext(jobConfig)
    }

    @JvmStatic
    fun toStreamDataTypes(dataFields: List<DataField>): StreamDataTypes {
        val allTypes = dataFields.associateBy({ it.fieldName }, { it.fieldType })
            .mapValues { (_, value) ->
                mappingTypeNameToInformation(value)
            }

        return StreamDataTypes(allTypes.keys.toTypedArray(), allTypes.values.toTypedArray())
    }

    @JvmStatic
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
            "TIMESTAMP_LTZ" -> Types.INSTANT
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