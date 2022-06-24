package com.rui.dp.prj

object NBProject {
    @JvmStatic
    fun main(args: Array<String>) {
        val process = NBStreamProcess()
        process.createTables()
        process.startProcess()
    }
}