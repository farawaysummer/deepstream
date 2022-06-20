package com.rui.ds.generator

import com.rui.ds.common.TableContext
import com.rui.ds.datasource.DatabaseSources

object TableGenerator {

    fun getGenerator(dsName: String, tableType: String): FlinkTableGenerator {
        val dataSourceConfig = DatabaseSources.getDataSourceConfig(dsName)
        return when (tableType) {
            TableContext.TABLE_TYPE_CDC -> JdbcCdcTableGenerator(dataSourceConfig!!)
            TableContext.TABLE_TYPE_DIM -> JdbcDimTableGenerator(dataSourceConfig!!)
            TableContext.TABLE_TYPE_KAFKA -> KafkaTableGenerator(dataSourceConfig!!)
            TableContext.TABLE_TYPE_KS -> KoalaspeedTableGenerator(dataSourceConfig!!)
            else -> JdbcTableGenerator(dataSourceConfig!!)
        }
    }
}