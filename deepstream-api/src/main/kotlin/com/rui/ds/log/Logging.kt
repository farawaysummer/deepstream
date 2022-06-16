package com.rui.ds.log

interface Logging {
    companion object {
        val LOGGER = LogManager.getLogger(Logging::class.java)
    }
}

inline fun <reified T : Logging> T.logger(): Logger =
    LogManager.getLogger(T::class.java)

