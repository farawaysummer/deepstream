package com.rui.dp.prj.toolkit

import com.rui.dp.prj.utils.ProjectToolkits

object Generator {
    @JvmStatic
    fun main(args: Array<String>) {
        val fields = mutableMapOf<String, String>()
        val sql = ProjectToolkits.getSql("selectDIP")
        val fieldContent = ProjectToolkits.generateResultFields("DIP", sql, fields)
        println("\n===============fields xml==============\n")
        println(fieldContent)

        println("\n===============insert table sql==============\n")

        val insertSql = ProjectToolkits.generateInsertSql("eigdb", "test","s_cli_recipe", fields)
        println(insertSql)

        println("\n===============create table sql==============\n")
        val tableSql = ProjectToolkits.generateFlinkTableSql("eigdb", "test", "s_cli_recipe")
        println(tableSql)
    }
}