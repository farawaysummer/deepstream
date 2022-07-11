package com.rui.dp.prj.base.log

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.slf4j.MDC

class DeepStreamLogger private constructor(clazz: Class<*>) {
    private val logger: Logger = LoggerFactory.getLogger(clazz)

    fun error(msg: Any?) {
        logger.error(msg?.toString())
    }

    fun error(message: Any?, t: Throwable?) {
        logger.error(message?.toString(), t)
    }

    fun error(message: String?, vararg params: Any?) {
        logger.error(message, *params)
    }

    fun errorJob(jobName: String, msg: Any?) {
        try {
            MDC.put("jobName", jobName)
            logger.error(msg?.toString())
        } finally {
            MDC.clear()
        }
    }

    fun errorJob(jobName: String, msg: Any?, t: Throwable?) {
        try {
            MDC.put("jobName", jobName)
            logger.error(msg?.toString(), t)
        } finally {
            MDC.clear()
        }
    }

    fun info(msg: Any?) {
        logger.info(msg?.toString())
    }

    fun info(message: String?, vararg params: Any?) {
        logger.info(message, *params)
    }

    fun infoJob(jobName: String, msg: Any?) {
        if (logger.isInfoEnabled) {
            try {
                MDC.put("jobName", jobName)
                logger.info(msg?.toString())
            } finally {
                MDC.clear()
            }
        }
    }

    fun infoJob(jobName: String, message: String?, vararg params: Any?) {
        if (logger.isInfoEnabled) {
            try {
                MDC.put("jobName", jobName)
                logger.info(message, *params)
            } finally {
                MDC.clear()
            }
        }
    }

    fun trace(message: String?, vararg params: Any?) {
        logger.trace(message, *params)
    }

    fun warn(message: String?, vararg params: Any?) {
        logger.warn(message, *params)
    }

    fun warn(msg: Any?) {
        logger.warn(msg?.toString())
    }

    fun warnJob(jobName: String, msg: Any?) {
        if (logger.isWarnEnabled) {
            try {
                MDC.put("jobName", jobName)
                logger.warn(msg?.toString())
            } finally {
                MDC.clear()
            }
        }
    }

    fun warnJob(jobName: String, message: String?, vararg params: Any?) {
        if (logger.isWarnEnabled) {
            try {
                MDC.put("jobName", jobName)
                logger.warn(message, *params)
            } finally {
                MDC.clear()
            }
        }
    }

    fun debug(msg: Any?) {
        logger.debug(msg?.toString())
    }

    fun debug(message: String?, vararg params: Any?) {
        if (logger.isDebugEnabled) {
            logger.debug(message, *params)
        }
    }

    fun debugJob(jobName: String, msg: Any?) {
        if (logger.isDebugEnabled) {
            try {
                MDC.put("jobName", jobName)
                logger.debug(msg?.toString())
            } finally {
                MDC.clear()
            }
        }
    }

    fun debugJob(jobName: String, message: String?, vararg params: Any?) {
        if (logger.isDebugEnabled) {
            try {
                MDC.put("jobName", jobName)
                logger.debug(message, *params)
            } finally {
                MDC.clear()
            }
        }
    }

    fun trace(msg: Any?) {
        logger.trace(msg?.toString())
    }

    fun traceJob(jobName: String, message: String?, vararg params: Any?) {
        if (logger.isTraceEnabled) {
            try {
                MDC.put("jobName", jobName)
                logger.trace(message, *params)
            } finally {
                MDC.clear()
            }
        }
    }

    fun isTraceEnabled(): Boolean {
        return logger.isTraceEnabled
    }

    fun isDebugEnabled(): Boolean {
        return logger.isDebugEnabled
    }

    fun isInfoEnabled(): Boolean {
        return logger.isInfoEnabled
    }

    fun isWarnEnabled(): Boolean {
        return logger.isWarnEnabled
    }

    fun isErrorEnabled(): Boolean {
        return logger.isErrorEnabled
    }

    companion object {
        @JvmStatic
        fun getLogger(clazz: Class<*>): DeepStreamLogger {
            return DeepStreamLogger(clazz)
        }
    }
}