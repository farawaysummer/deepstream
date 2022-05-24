package com.rui.ds.job

import com.google.common.collect.HashMultimap
import com.google.common.collect.Queues
import com.rui.ds.ProcessContext
import com.rui.ds.common.*
import com.rui.ds.datasource.DatabaseSources
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.SqlDialect
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment
import org.apache.flink.types.Row
import java.io.File

data class JobConfig(
    val jobMode: String = "STREAM",
    val miniBatchEnabled: Boolean = false,
    val enableWebUI: Boolean = false,

    // kafka config, required
    val kafkaServer: String = "",

    // cache database config
    val cacheDBUrl: String = "",
    val cacheDBUser: String = "",
    val cacheDBPassword: String = "",
)

class DeepStreamJob(
    private val steps: Map<String, DataProcessStep>,
    private val hops: List<Hop>
) {
    private val startSteps: List<DataProcessStep> = steps.values.filterIsInstance<InputStep>()

    internal fun initDebugContext(jobConfig: JobConfig): ProcessContext {
        val fsSettings = if (jobConfig.jobMode == "STREAM") {
            EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build()
        } else {
            EnvironmentSettings.newInstance()
                .inBatchMode()
                .build()
        }

        val configuration = Configuration()

        if (jobConfig.miniBatchEnabled) {
            configuration.setString("table.exec.mini-batch.enabled", "true") // enable mini-batch optimization
            configuration.setString(
                "table.exec.mini-batch.allow-latency",
                "5 s"
            ) // use 5 seconds to buffer input records
            configuration.setString("table.exec.mini-batch.size", "5000")
            configuration.setString(
                "table.optimizer.agg-phase-strategy",
                "TWO_PHASE"
            ) // enable two-phase, i.e. local-global aggregation
        }

        val env = if (jobConfig.enableWebUI) {
            configuration.setInteger("rest.port", 8082)
            StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration)
        } else {
            StreamExecutionEnvironment.createLocalEnvironment(configuration)
        }

        env.parallelism = 1
        val checkpointStorage = File("./flink/checkpoint")
        checkpointStorage.mkdirs()
        env.checkpointConfig.checkpointStorage =
            FileSystemCheckpointStorage("file://${checkpointStorage.absolutePath}")

        env.enableCheckpointing(10000L)  //头和头
        env.checkpointConfig.checkpointingMode = CheckpointingMode.EXACTLY_ONCE
        env.checkpointConfig.checkpointTimeout = 10000L
        env.checkpointConfig.maxConcurrentCheckpoints = 2
        env.checkpointConfig.minPauseBetweenCheckpoints = 10000L

        val tableEnv = StreamTableEnvironment.create(env, fsSettings)
        tableEnv.config.sqlDialect = SqlDialect.DEFAULT

        return ProcessContext(
            env = env,
            tableEnv = tableEnv
        )
    }

    override fun toString(): String {
        return "DeepStreamJob(steps=$steps, hops=$hops)"
    }

    companion object {
        fun of(configs: List<DataSourceConfig>, steps: List<Step>, hops: List<Hop>): DeepStreamJob {
            configs.forEach {
                DatabaseSources.registryDataSource(it)
            }

            steps.forEach {
                it.initStep()
            }

            return DeepStreamJob(
                steps.associateBy({ it.name }, { it as DataProcessStep }),
                hops
            )
        }
    }

    fun visit(processContext: ProcessContext) {
        // find union steps
        var unionSteps: MutableMap<String, UnionFrom> = mutableMapOf()
        val hopMap = HashMultimap.create<String, String>()

        for (hop in hops) {
            val toStep = hop.toStep
            var fromInfo = unionSteps[toStep]
            if (fromInfo == null) {
                fromInfo = UnionFrom(toStep)
                unionSteps[toStep] = fromInfo
            }
            fromInfo.fromSteps.add(hop.fromStep)

            hopMap.put(hop.fromStep, hop.toStep)
        }

        unionSteps = unionSteps.filterValues { it.fromSteps.size > 1 }.toMutableMap()

        val stepQueue = Queues.newConcurrentLinkedQueue<String>()
        val dataMap = mutableMapOf<String, DataContext>()
        // init input steps
        startSteps.forEach {
            stepQueue.offer(it.name)
            // input steps with empty data context
            dataMap[it.name] = DataContext.EMPTY_CONTEXT
        }

        // visit all unprocessed step in DAG
        loop@ while (stepQueue.isNotEmpty()) {
            // find first unprocessed step
            var cursor = stepQueue.poll()

            // get step data, if it's input, get empty data context
            // otherwise, get temp data stored before
            val cursorData = dataMap[cursor] ?: DataContext.EMPTY_CONTEXT
            // process current step with current data
            var nextStepData = steps[cursor]!!.processStreamData(cursorData, processContext)

            // get next steps
            // dfs from cursor until endpoint or union point
            var nextStepNames = hopMap[cursor]
            while (!nextStepNames.isNullOrEmpty()) {
                // is endpoint
                val nextStepName = if (nextStepNames.size > 1) { // is separator step
                    // for separator step, offer next steps and get first step of them as next step
                    stepQueue.addAll(nextStepNames)
                    // generate all separator data and cache
                    nextStepNames.forEach {
                        dataMap[it] = separatorData(nextStepData, it)
                    }

                    stepQueue.poll()
                } else {
                    // direct step, just get it
                    nextStepNames.first()
                }

                // check if the next step is union step
                if (unionSteps.contains(nextStepName)) {
                    val unionStep = unionSteps[nextStepName]!!
                    // current step is finished, remove it from union condition
                    unionStep.unfinished.remove(cursor)
                    // cache current processed data for union
                    dataMap[cursor] = nextStepData
                    // all branch are finished, union those data for union step processing
                    if (unionStep.unfinished.isEmpty()) {
                        nextStepData = unionData(dataMap, unionStep.fromSteps)
                    } else {
                        // some branches unfinished
                        continue@loop
                    }
                }

                // process next step and move cursor
                nextStepData = steps[nextStepName]!!.processStreamData(nextStepData, processContext)

                cursor = nextStepName

                nextStepNames = hopMap[cursor]
            }
        }
    }

    private fun unionData(dataMap: MutableMap<String, DataContext>, fromSteps: Set<String>): DataContext {
        val firstStep = fromSteps.first()
        val firstData = dataMap.remove(firstStep)!!

        val stream = firstData.stream!!

        return DataContext(
            stream = stream.union(
                *(fromSteps
                    .drop(1)
                    .map { dataMap.remove(it)!!.stream }
                    .toTypedArray())
            )
        )
    }

    /**
     *  get side output from main stream in data
     */
    private fun separatorData(data: DataContext, stepName: String): DataContext {
        val useSideOutput = data.outputTags.containsKey(stepName)
        val stream = if (useSideOutput) {
            (data.stream as SingleOutputStreamOperator<Row>).getSideOutput(data.outputTags[stepName])
        } else {
            data.stream!!
        }

        return DataContext(stream)
    }


}

data class UnionFrom(
    val step: String,
    val fromSteps: MutableSet<String> = mutableSetOf(),
    val unfinished: MutableSet<String> = mutableSetOf(),
)
