package com.rui.ds.facade.kettle

import org.junit.Assert.*
import org.junit.Test
import java.io.File

class KettleStreamRunnerTest {

    @Test
    fun `test ktr execution`() {
        val url = javaClass.getResource("/DI_ADI_EXPSET_LIST_eig.ktr")
        assertNotNull(url)

        val ktrFile = File(url!!.toURI()).absolutePath
        KettleStreamRunner.executeKtr(ktrFile)
    }

}