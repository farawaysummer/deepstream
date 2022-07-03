package com.rui.dp.prj.base

import com.rui.ds.steps.transform.dm.DPTransformGateway
import com.ruisoft.eig.transform.transformer.Transformer
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.types.Row
import org.slf4j.LoggerFactory
import kotlin.math.log

class ValueMappingFunction(
    private val jobIds: List<Long>,
    private val fields: Array<String>
) : RichMapFunction<Row, Row>() {

    @Transient
    private var transformGateway: DPTransformGateway? = null

    @Transient
    private var transforms: Map<String, Transformer> = mutableMapOf()

    override fun open(parameters: Configuration?) {
        super.open(parameters)
        transformGateway = DPTransformGateway.gateway
        val allTrans = mutableMapOf<String, Transformer>()
        for (jobId in jobIds) {
            val trans = transformGateway!!.match(jobId, fields)
            allTrans.putAll(trans)
        }

        this.transforms = allTrans.toMap()
    }

    override fun map(value: Row): Row {
        val transFields = transforms.keys
        if (logger.isDebugEnabled) {
            logger.debug("Ready to Mapping Fields: $transFields")
        }
        transFields.forEach { field ->
            val fieldValue = value.getField(field)
            val transform = transforms[field]
            if (fieldValue != null && transform != null) {
                val result = transform.transform(field, arrayOf(field), arrayOf(fieldValue))
                if (result.isSuccess) {
                    if (logger.isDebugEnabled) {
                        logger.debug("Mapping field $field from $fieldValue to ${result.resultValue}")
                    }
                    value.setField(field, result.resultValue)
                }
            }
        }

        return value
    }

    companion object {
        val logger = LoggerFactory.getLogger(ValueMappingFunction::class.java)
    }
}