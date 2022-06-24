package com.rui.dp.prj.nb

import com.rui.dp.prj.nb.mz.NBStreamProcessCDR


object NBProject {
    @JvmStatic
    fun main(args: Array<String>) {
        val process = NBStreamProcessCDR()
        process.init()
        process.prepare()
        process.start()
        process.clean()
    }
}