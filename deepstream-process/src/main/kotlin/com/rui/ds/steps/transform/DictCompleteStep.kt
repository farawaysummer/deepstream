package com.rui.ds.steps.transform

import com.rui.ds.ProcessContext
import com.rui.ds.common.DataContext
import com.rui.ds.common.StepMeta
import com.rui.ds.common.TransformStep
import com.rui.ds.steps.transform.dm.DPTransformGateway
import com.ruisoft.eig.transform.transformer.Transformer
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.types.Row

class DictCompleteStep(name: String, override val meta: DictCompleteStepMeta) : TransformStep(name, meta) {
    private val dictFunction: DictMappingFunction = DictMappingFunction(meta.jobId, meta.fields.toTypedArray())

    override fun process(data: DataContext, process: ProcessContext): DataContext {
        val stream = toStream(data, process)!!

        return dataByStream(stream.map(dictFunction))
    }
}

data class DictCompleteStepMeta(
    val jobId: Long,
    val fields: List<String>
) : StepMeta

class DictMappingFunction(
    val jobId: Long,
    val fields: Array<String>
) : RichMapFunction<Row, Row>() {

    @Transient
    private var transformGateway: DPTransformGateway? = null

    @Transient
    private var transforms: Map<String, Transformer> = emptyMap()

    override fun open(parameters: Configuration?) {
        super.open(parameters)
        transformGateway = DPTransformGateway.gateway
        transforms = transformGateway!!.match(jobId, fields)
    }

    override fun map(value: Row): Row {
        val transFields = transforms.keys
        transFields.forEach { field ->
            val fieldValue = value.getField(field)
            val transform = transforms[field]
            if (fieldValue != null && transform != null) {
                val result = transform.transform(field, arrayOf(field), arrayOf(fieldValue))
                if (result.isSuccess) {
                    value.setField(field, result.resultValue)
                }
            }
        }

        return value
    }
}