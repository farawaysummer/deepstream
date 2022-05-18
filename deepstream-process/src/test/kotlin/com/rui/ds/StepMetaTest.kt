package com.rui.ds

import com.rui.ds.common.DataSourceConfig
import com.rui.ds.datasource.DatabaseSources
import org.junit.Test
import java.sql.ResultSet

class StepMetaTest {

    @Test
    fun `check meta info from query sql`() {
        val factTableName = "r_mpi_patientinfo"
        val nationDimTable = "r_dict_nation"
        val dimTableName = "r_dict_medicalpayment"

        val selectSQL = """
select r.PATIENT_ID,
       r.MEDICAL_TYPE_CODE,
       r.SEX_NAME,
       r.NAME,
       (select d.NAME from $dimTableName as d where r.MEDICAL_TYPE_CODE = d.CODE) as nc
from $factTableName 
         as r limit 1
        """.trimIndent()

        val dataSourceConfig = DataSourceConfig(
            name = "test",
            dbName = "eigdb",
            host = "192.168.3.168",
            port = 3216,
            type = "mysql",
            username = "reseig",
            password = "mu-xPL7C"
        )

        DatabaseSources.registryDataSource(dataSourceConfig)
        val connection = DatabaseSources.getConnection("test")!!
        val resultSet: ResultSet = connection.metaData.getColumns(
            "", "",
            selectSQL, ""
        )

        while (resultSet.next()) {
            val name = resultSet.getString("COLUMN_NAME")
            println(name)
        }
//        val result = DataBase.getQueryFieldsFromPreparedStatement(connection, selectSQL)
//        println(result)
    }

    @Test
    fun `check output format`() {
        val factTableName = "r_mpi_patientinfo"
        val nationDimTable = "r_dict_nation"
        val dimTableName = "r_dict_medicalpayment"

        val selectSQL = """
select r.PATIENT_ID,
       r.MEDICAL_TYPE_CODE,
       r.SEX_NAME,
       r.NAME,
       (select d.NAME from $dimTableName as d where r.MEDICAL_TYPE_CODE = d.CODE) as nc
from $factTableName 
        where field = ${'$'}{BEGIN_DATE} as r limit 1
        """.trimIndent()

    }

}