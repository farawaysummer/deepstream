package com.rui.ds.facade.kettle

import com.rui.ds.common.DataSourceConfig
import com.rui.ds.common.Hop
import com.rui.ds.common.Step
import com.rui.ds.job.DeepStreamJob
import com.rui.ds.parser.JobMetaParser
import org.dom4j.Element
import org.dom4j.io.SAXReader
import org.reflections.Reflections
import java.io.StringReader
import java.math.BigInteger


/**
 * KTR 文件解析为流程
 */
class KettleJobParser : JobMetaParser<String> {

    private val stepParsers: Map<String, KettleStepParser>

    init {
        val reflections = Reflections(
            "com.rui.ds.facade.kettle.steps"
        )
        val tempMap = mutableMapOf<String, KettleStepParser>()
        val parserClazz = reflections.getSubTypesOf(KettleStepParser::class.java)
        parserClazz.forEach {
            val stepInfos = it.getAnnotationsByType(KettleStep::class.java)
            if (stepInfos.isNotEmpty()) {
                val parser = it.newInstance()
                for (stepInfo in stepInfos) {
                    tempMap[stepInfo.name] = parser
                }
            }
        }

        stepParsers = tempMap.toMap()
    }

    override fun parse(source: String): DeepStreamJob {
        // read from xml
        val reader = SAXReader()
        val document = reader.read(StringReader(source))
        val rootElement = document.rootElement

        // parse connection
        val connections = rootElement.elements("connection")
        val configs = connections.map {
            parseConnection(it)
        }

        // parse step
        val stepElements = rootElement.elements("step")
        val steps = stepElements.map {
            parseStep(it)
        }

        // parse hop
        val orderElement = rootElement.element("order")
        val hopElements = orderElement.elements("hop")
        val hops = hopElements.map {
            parseHop(it)
        }

        return DeepStreamJob.of(configs, steps, hops)
    }

    private fun parseHop(element: Element): Hop {
        return Hop(
            fromStep = element.elementText("from"),
            toStep = element.elementText("to"),
            enabled = (element.elementText("enabled") == "Y")
        )
    }

    private fun parseConnection(conElement: Element): DataSourceConfig {
        val name = conElement.elementText("name")
        val host = conElement.elementText("server")
        val port = conElement.elementText("port")
        val type = conElement.elementText("type").lowercase()
        val username = conElement.elementText("username")
        val password = decryptPassword(conElement.elementText("password"))
        val database = conElement.elementText("database")

        val attributesEle = conElement.element("attributes")
        val elements = attributesEle.elements("attribute")
        val attributes =
            elements.associateBy({ it.elementText("code") }, { it.elementText("attribute") })

        return DataSourceConfig(
            name = name,
            dbName = database,
            username = username,
            password = password,
            type = type,
            host = host,
            port = port.toInt(),
            properties = attributes
        )
    }

    private fun parseStep(stepElement: Element): Step {
        val type = stepElement.elementText("type")
        val parser = stepParsers[type] ?: throw RuntimeException("Unsupported step type $type.")

        return parser.parse(stepElement)
    }

    private fun decryptPassword(encrypted: String?): String {
        if (encrypted.isNullOrEmpty()) {
            return ""
        }

        if (!encrypted.startsWith("Encrypted ")) {
            return encrypted
        }

        val biConfuse = BigInteger(seed)
        return try {
            val biR1 = BigInteger(encrypted, 16)
            val biR0 = biR1.xor(biConfuse)
            String(biR0.toByteArray())
        } catch (e: Exception) {
            ""
        }
    }

    companion object {
        const val seed: String = "0933910847463829827159347601486730416058"
    }
}