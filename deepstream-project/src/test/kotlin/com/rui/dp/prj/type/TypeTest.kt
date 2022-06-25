package com.rui.dp.prj.type

import com.rui.dp.prj.base.DeepStreamHelper
import com.rui.ds.StreamDataTypes
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.table.api.DataTypes
import org.apache.flink.types.Row

object TypeTest {
    @JvmStatic
    fun main(args: Array<String>) {
        val context = DeepStreamHelper.initEnv()

        // create table1
        DeepStreamHelper.executeSQL(
            context, "CREATE TABLE F_SRC_RECORD (\n" +
                    "`AUTO_ID` STRING,\n" +
                    "`REC_ID` BIGINT,\n" +  // BIGINT TO LONG
                    "`UPDATE_TIME` TIMESTAMP(6),\n" +
                    "`REC_DATE` DATE,\n" +
                    "`QTY` DECIMAL(10,2),\n" +
                    "PRIMARY KEY (`AUTO_ID`) NOT ENFORCED\n" +
                    ") \n" +
                    " WITH (\n" +
                    "'connector' = 'jdbc',\n" +
                    "'url' = 'jdbc:oracle:thin:@192.168.3.164:1521:orcl',\n" +
                    "'table-name' = 'DIP.F_SRC_RECORD',\n" +
                    "'username' = 'dip',\n" +
                    "'password' = 'ruisoft',\n" +
                    "'driver' = 'oracle.jdbc.driver.OracleDriver'" +
                    ")"
        )

        // create table2
        DeepStreamHelper.executeSQL(
            context, "CREATE TABLE F_TAR_RECORD (\n" +
                    "`AUTO_ID` VARCHAR,\n" +
                    "`REC_ID` VARCHAR,\n" +
                    "`UPDATE_TIME` TIMESTAMP,\n" +
                    "`REC_DATE` DATE,\n" +
                    "`QTY` DECIMAL(10,2),\n" +
                    "`ADDF` VARCHAR,\n" +
                    "PRIMARY KEY (`AUTO_ID`) NOT ENFORCED\n" +
                    ") \n" +
                    " WITH (\n" +
                    "'connector' = 'jdbc',\n" +
                    "'url' = 'jdbc:mysql://192.168.3.168:3216/test?useSSL=false&autoReconnect=true',\n" +
                    "'table-name' = 'F_TAR_RECORD',\n" +
                    "'username' = 'reseig',\n" +
                    "'password' = 'mu-xPL7C',\n" +
                    "'driver' = 'com.mysql.cj.jdbc.Driver')"
        )

        val queryTable = DeepStreamHelper.executeQuery(
            context,
            "select AUTO_ID, REC_ID, UPDATE_TIME, REC_DATE, QTY from F_SRC_RECORD"
        )

        Types.BYTE

        val stream = context.tableEnv.toChangelogStream(queryTable)

        val function = TestFunction()
        val processed = stream.map(function)
            .returns(Types.ROW_NAMED(
                arrayOf(
                    "AUTO_ID",
                    "REC_ID",
                    "UPDATE_TIME",
                    "REC_DATE",
                    "QTY",
                    "ADDF"
                ),
                Types.STRING, Types.LONG, Types.LOCAL_DATE_TIME, Types.LOCAL_DATE, Types.BIG_DEC, Types.STRING
            ))
//            .returns(dataTypes.toTypeInformation())

        context.tableEnv.createTemporaryView("DTable", processed)
        // select from table1

//        DeepStreamHelper.executeSQL(context, "select * from DTable").print()

        // insert into table2
        val insertSql = "insert into F_TAR_RECORD (AUTO_ID, REC_ID, UPDATE_TIME, REC_DATE, QTY) select AUTO_ID, " +
                "CAST(REC_ID AS STRING) AS REC_ID, " +
                "UPDATE_TIME, " +
                "REC_DATE, " +
                "QTY from DTable"
        DeepStreamHelper.executeSQL(context, insertSql)
    }
}

class TestFunction : MapFunction<Row, Row> {
    override fun map(value: Row): Row {

        val addFields = Row.of("9511")
        return Row.join(value, addFields)
    }
}