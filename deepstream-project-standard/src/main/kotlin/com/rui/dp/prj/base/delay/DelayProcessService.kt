package com.rui.dp.prj.base.delay

import com.google.common.cache.CacheBuilder
import com.google.common.cache.CacheLoader
import com.google.common.cache.LoadingCache
import com.google.common.cache.RemovalListener
import com.rui.ds.ks.delay.DelayRecordDispatcher
import com.rui.ds.ks.delay.DelayServiceKey
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit

object DelayProcessService {
    private val services: LoadingCache<DelayServiceKey, ScheduledExecutorService> = CacheBuilder.newBuilder()
        .removalListener(
            RemovalListener<DelayServiceKey, ScheduledExecutorService> { notification -> notification.value.shutdown() }
        )
        .build(object : CacheLoader<DelayServiceKey, ScheduledExecutorService>() {
            override fun load(key: DelayServiceKey): ScheduledExecutorService {
                val publisher = DelayRecordDispatcher(key.servers, key.level)
                val executor = Executors.newSingleThreadScheduledExecutor()
                executor.scheduleAtFixedRate(publisher, 0, 1L, TimeUnit.SECONDS)

                return executor
            }
        })

    @JvmStatic
    fun registerDelayProcess(publisherKey: DelayServiceKey) {
        services.get(publisherKey)
    }

    @JvmStatic
    fun close() {
        services.invalidateAll()
    }
}