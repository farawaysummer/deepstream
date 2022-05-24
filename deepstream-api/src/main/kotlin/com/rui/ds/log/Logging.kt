package com.rui.ds.log

interface Logging

inline fun <reified T : Logging> T.logger(): Logger =
    LogManager.getLogger(T::class.java)