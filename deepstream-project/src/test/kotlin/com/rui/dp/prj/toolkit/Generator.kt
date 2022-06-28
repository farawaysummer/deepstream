package com.rui.dp.prj.toolkit

import com.rui.dp.prj.utils.ProjectToolkits

object Generator {
    @JvmStatic
    fun main(args: Array<String>) {
        val fields = mutableMapOf<String, String>()
        val sql = ProjectToolkits.getSql("selectC01")
        val fieldContent = ProjectToolkits.generateResultFields("DIP", sql, fields)
        println("\n===============fields xml==============\n")
        println(fieldContent)

        println("\n===============insert table sql==============\n")

        val insertSql = ProjectToolkits.generateInsertSql("DIP", "DIP","S_CLI_RECIPE_CDC", fields)
        println(insertSql)

//        println("\n===============create table sql==============\n")
//        val tableSql = ProjectToolkits.generateFlinkTableSql("eigdb", "test", "c01040000")
//        println(tableSql)
    }
}