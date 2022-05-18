package com.rui.ds

import org.apache.flink.util.OutputTag
import org.junit.Test

class SideOutputTest {

    @Test
    fun `check side tag`() {
        val tag = object : OutputTag<String>("side-output") {}
        println(tag::class)
    }

}