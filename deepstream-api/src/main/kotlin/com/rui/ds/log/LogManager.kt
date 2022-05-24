package com.rui.ds.log

import org.apache.logging.log4j.LogManager
import kotlin.reflect.KClass

/**
 * @author lunar
 */
object LogManager {
    fun getLogger(clazz: Class<*>): Logger {
        val logger = LogManager.getLogger(clazz)
        return Logger(logger)
    }

    fun getLogger(clazz: KClass<*>): Logger {
        val logger = LogManager.getLogger(clazz.java)
        return Logger(logger)
    }

    fun getLogger(loggerName: String): Logger {
        val logger = LogManager.getLogger(loggerName)
        return Logger(logger)
    }

}