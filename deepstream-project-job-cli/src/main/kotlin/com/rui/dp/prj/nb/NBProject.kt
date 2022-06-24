package com.rui.dp.prj.nb

import com.rui.dp.prj.nb.cli.NBStreamProcessCLI

object NBProject {
    @JvmStatic
    fun main(args: Array<String>) {
        val process = NBStreamProcessCLI()
        process.init()
        process.prepare()
        process.start()
        process.clean()
    }
}