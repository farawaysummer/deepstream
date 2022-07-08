package com.rui.dp.prj.base.job

import com.rui.ds.common.DataSourceConfig
import org.dom4j.Element
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.math.BigInteger

abstract class JobDataLoader {
    abstract fun loadResources(resourceName: String): Element

    fun loadDatasource(dsResource: String): Map<String, DataSourceConfig> {
        val rootElement = loadResources(dsResource)
        val connections = rootElement.elements("connection")

        val configs = connections.map {
            parseConnection(it)
        }

        return configs.associateBy({ it.name }, { it })
    }

    fun loadSql(sqlResource: String): Map<String, String> {
        val rootElement = loadResources(sqlResource)
        val sqlElements = rootElement.elements("sql")
        return sqlElements.associateBy({ it.attributeValue("name") }, { it.text })
    }

    internal fun loadTableType(element: Element): TableType {
        val typeName = element.attributeValue("name")
        val ignoreKey = element.attributeValue("ignoreKey")?: "false"

        val typeProperties = element.elements("config")
            .associateBy({ it.attributeValue("name") }, { it.attributeValue("value") })
        return TableType(typeName, typeProperties, ignoreKey.toBoolean())
    }

    internal fun loadTableRef(refName: String): List<DataField> {
        val rootElement = loadResources("table-$refName")
        val fields = rootElement.elements("field").map {
            DataField(
                it.attributeValue("name"),
                it.attributeValue("type"),
                it.attributeValue("isKey")?.toBoolean() ?: false
            )
        }

        return fields
    }

    fun parseConnection(conElement: Element): DataSourceConfig {
        val name = conElement.elementText("name")
        val host = conElement.elementText("server")
        val port = conElement.elementText("port")
        val type = conElement.elementText("type").lowercase()
        val username = conElement.elementText("username")
        val password = decryptPassword(conElement.elementText("password"))
        val database = conElement.elementText("database")

        val attributesEle = conElement.element("attributes")
        val attributes = if (attributesEle != null) {
            val elements = attributesEle.elements("attribute")
            elements.associateBy({ it.elementText("code") }, { it.elementText("attribute") })
        } else {
            emptyMap()
        }

        val subType = conElement.element("subType")?.text

        val subTypeValue = subType?.toInt() ?: DataSourceConfig.DATABASE_SUBTYPE_DEFAULT

        return DataSourceConfig(
            name = name,
            dbName = database,
            username = username,
            password = password,
            type = type,
            host = host,
            port = port.toInt(),
            properties = attributes,
            subType = subTypeValue
        )
    }

    private fun decryptPassword(encrypted: String?): String {
        if (encrypted.isNullOrEmpty()) {
            return ""
        }

        if (!encrypted.startsWith("Encrypted ")) {
            return encrypted
        }

        val encPassword = encrypted.substring("Encrypted ".length)
        val biConfuse = BigInteger(seed)
        return try {
            val biR1 = BigInteger(encPassword, 16)
            val biR0 = biR1.xor(biConfuse)
            String(biR0.toByteArray())
        } catch (e: Exception) {
            ""
        }
    }

    companion object {
        internal const val seed: String = "0933910847463829827159347601486730416058"
        val logger: Logger = LoggerFactory.getLogger(JobDataLoader::class.java)
    }
}