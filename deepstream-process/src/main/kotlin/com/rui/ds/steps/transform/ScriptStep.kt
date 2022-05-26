package com.rui.ds.steps.transform

import com.rui.ds.FieldInfo
import com.rui.ds.ProcessContext
import com.rui.ds.common.DataContext
import com.rui.ds.common.StepMeta
import com.rui.ds.common.TransformStep
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.types.Row
import javax.script.Compilable
import javax.script.CompiledScript
import javax.script.ScriptEngineManager

class ScriptStep(name: String, override val meta: ScriptStepMeta): TransformStep(name, meta) {

    private val scriptFunction: JsScriptFunction = JsScriptFunction(meta.script, meta.outputFields)

    override fun process(data: DataContext, process: ProcessContext): DataContext {
        val stream = toStream(data, process)!!

        return dataByStream(stream.map(scriptFunction))
    }
}

data class ScriptStepMeta(
    val script: String,
    val outputFields: List<FieldInfo>
): StepMeta

class JsScriptFunction(
    private val script: String,
    private val outputFields: List<FieldInfo>
) : MapFunction<Row, Row> {
    @Transient
    private val engine = ScriptEngineManager().getEngineByName("nashorn")
    @Transient
    private val compiledScript = (engine as Compilable).compile(script)

    override fun map(value: Row): Row {
        // 将列值绑定到引擎运行环境
        // 执行脚本
        // 将绑定值回写到列值中

        val bindings = compiledScript.engine.createBindings()

        val fields = value.getFieldNames(true)!!
        for (fieldName in fields) {
            bindings[fieldName] = value.getField(fieldName)
        }

        val appendFields = outputFields.filter { !fields.contains(it.name) }
        appendFields.forEach{
            bindings[it.name] = null
        }

        compiledScript.eval(bindings)

        val addInfo = Row.of(*appendFields.map { bindings[it.name] }.toTypedArray())
        fields.forEach {
            value.setField(it,bindings[it])
        }

        return Row.join(value, addInfo)
    }
}