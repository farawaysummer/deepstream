package com.rui.ds.common

import com.rui.ds.ProcessContext
import com.rui.ds.StreamDataTypes
import com.rui.ds.log.Logging

import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.table.api.Table
import org.apache.flink.types.Row

interface Step: Logging {
    val name: String
    val inputDataType: StreamDataTypes
    val outputDataType: StreamDataTypes

    fun initStep()

    fun beforeProcess(data: DataContext, process: ProcessContext) {}

    fun process(data: DataContext, process: ProcessContext): DataContext

    fun afterProcess(data: DataContext, process: ProcessContext) {}

    fun dataByTable(table: Table): DataContext {
        val data = DataContext(table)

        val columns = table.resolvedSchema.columns
        data.types = StreamDataTypes.of(
            columns.map { it.name }.toTypedArray(),
            columns.map { it.dataType }.toTypedArray()
        )

        return data
    }

    fun dataByStream(stream: DataStream<Row>, types: StreamDataTypes = StreamDataTypes.INIT): DataContext {
        return if (types != StreamDataTypes.INIT && stream is SingleOutputStreamOperator) {
            DataContext(stream.returns(types.toTypeInformation()))
        } else {
            DataContext(stream)
        }
    }

    fun toStream(data: DataContext, process: ProcessContext): DataStream<Row>? {
        val stream =
            when (data.contextType) {
                DataContext.CONTEXT_TYPE_TABLE -> {
                    process.tableEnv.toChangelogStream(data.table) // TODO 要根据执行类型（批量或流），决定生成的流是Append-only或Changelog
                }
                DataContext.CONTEXT_TYPE_STREAM -> {
                    return data.stream!!
                }
                else -> {
                    null
                }
            }

        return stream
    }

    fun toTable(data: DataContext, process: ProcessContext): Table? {
        if (data.contextType == DataContext.CONTEXT_TYPE_TABLE) {
            return data.table
        }

        if (data.contextType == DataContext.CONTEXT_TYPE_STREAM) {
            return process.tableEnv.fromDataStream(data.stream)
        }

        return null
    }
}

abstract class DataProcessStep(
    override val name: String,
    open val meta: StepMeta
) : Step {

    override val inputDataType: StreamDataTypes = StreamDataTypes.INIT
    override val outputDataType: StreamDataTypes = StreamDataTypes.INIT

    override fun initStep() {

    }

    fun processStreamData(data: DataContext, process: ProcessContext): DataContext {
        beforeProcess(data, process)

        val context = process(data, process)

        afterProcess(data, process)

        return context
    }

    open fun dispose() {

    }

    override fun toString(): String {
        return "Step(name='$name', meta=$meta, inputDataType=$inputDataType, outputDataType=$outputDataType)"
    }
}

abstract class InputStep(name: String, meta: StepMeta) : DataProcessStep(name, meta)
abstract class TransformStep(name: String, meta: StepMeta) : DataProcessStep(name, meta)
abstract class OutputStep(name: String, meta: StepMeta) : DataProcessStep(name, meta)

interface StepMeta


