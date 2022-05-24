package com.rui.ds.log

import org.apache.logging.log4j.Level
import org.apache.logging.log4j.Logger
import org.apache.logging.log4j.MarkerManager

/**
 * @author lunar
 */
open class Logger internal constructor(private var logger: Logger) {
    fun error(msg: Any?) {
        logger.error(msg)
    }

    fun error(message: Any?, t: Throwable?) {
        logger.error(message, t)
    }

    fun error(message: String, vararg params: Any?) {
        logger.error(message, *params)
    }

    fun info(msg: Any?) {
        logger.info(msg)
    }

    fun info(message: String, vararg params: Any?) {
        logger.info(message, *params)
    }

    fun trace(message: String, vararg params: Any?) {
        logger.trace(message, *params)
    }

    fun warn(message: String, vararg params: Any?) {
        logger.warn(message, *params)
    }

    fun warn(msg: Any?) {
        logger.warn(msg)
    }

    fun debug(msg: Any?) {
        logger.debug(msg)
    }

    fun debug(message: String, vararg params: Any?) {
        if (logger.isDebugEnabled) {
            logger.debug(message, *params)
        }
    }

    fun trace(msg: Any?) {
        logger.trace(msg)
    }

    val isTraceEnabled: Boolean
        get() = logger.isEnabled(Level.TRACE)
    val isDebugEnabled: Boolean
        get() = logger.isEnabled(Level.DEBUG)
    val isInfoEnabled: Boolean
        get() = logger.isEnabled(Level.INFO)
    val isWarnEnabled: Boolean
        get() = logger.isEnabled(Level.WARN)
    val isErrorEnabled: Boolean
        get() = logger.isEnabled(Level.ERROR)

}