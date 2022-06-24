package com.rui.dp.prj.nb

import com.rui.dp.prj.nb.cf.NBStreamProcessCF

object NBProject {
    @JvmStatic
    fun main(args: Array<String>) {
        val process = NBStreamProcessCF()
        process.init()
        process.prepare()
        process.start()
        process.clean()
    }
}