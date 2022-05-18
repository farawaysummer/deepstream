package com.rui.ds

import org.junit.Test
import org.junit.jupiter.api.Assertions.assertEquals
import javax.script.Compilable
import javax.script.ScriptEngineManager

internal class ScriptTest {

    @Test
    fun `test js engine return`() {
        val scriptContent = """
            TREATMENT_GROUP_CODE = T_TREATMENT_GROUP_CODE;
            TREATMENT_GROUP_NAME = T_TREATMENT_GROUP_NAME;
        """.trimIndent()
        val engine = ScriptEngineManager().getEngineByName("nashorn")
        val script = (engine as Compilable).compile(scriptContent)
        val bindings = engine.createBindings()
        bindings["T_TREATMENT_GROUP_CODE"] = 1
        bindings["T_TREATMENT_GROUP_NAME"] = 2
        bindings["TREATMENT_GROUP_CODE"] = null
        bindings["TREATMENT_GROUP_NAME"] = null

        script.eval(bindings)

        println(bindings["TREATMENT_GROUP_NAME"])
        assertEquals(1, bindings["TREATMENT_GROUP_CODE"])
    }
}