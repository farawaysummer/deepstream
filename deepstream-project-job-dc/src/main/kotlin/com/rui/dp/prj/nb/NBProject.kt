package com.rui.dp.prj.nb

import com.rui.dp.prj.nb.dc.NBStreamProcessDC

object NBProject {
    @JvmStatic
    fun main(args: Array<String>) {
        val process = NBStreamProcessDC()
        process.init()
        process.prepare()
        process.start()
        process.clean()
    }
}