package com.rui.ds.steps.transform.dm

import com.google.common.cache.CacheBuilder
import com.google.common.cache.CacheLoader
import com.google.common.cache.LoadingCache
import com.rui.ds.common.DataSourceConfig
import com.rui.ds.datasource.DatabaseSources
import com.ruisoft.eig.transform.TransformGateway
import com.ruisoft.eig.transform.event.EIGCTRLTransformerEventListener
import com.ruisoft.eig.transform.event.EIGMCTransformerEventListener
import com.ruisoft.eig.transform.repository.JdbcTransformerRepository
import com.ruisoft.eig.transform.repository.JdbcValueDomainRepository
import com.ruisoft.eig.transform.repository.TransformerRepository
import com.ruisoft.eig.transform.repository.ValueDomainRepository
import com.ruisoft.eig.transform.strategy.DefaultTransformPriorityStrategy
import com.ruisoft.eig.transform.strategy.TransformPriorityStrategy
import com.ruisoft.eig.transform.transformer.Transformer
import com.ruisoft.eig.transform.valuedomain.ValueDomain
import java.util.*
import javax.sql.DataSource

class DPTransformGateway private constructor(
    eigmcDataSource: DataSource,
    ctrlDataSource: DataSource,
) :
    TransformGateway {
    private val valueDomainRepository: ValueDomainRepository
    private val transformerRepository: TransformerRepository
    private val strategyCache: LoadingCache<Long, TransformPriorityStrategy>

    init {
        valueDomainRepository = DPValueDomainRepository(JdbcValueDomainRepository(eigmcDataSource))
        transformerRepository = JdbcTransformerRepository(eigmcDataSource, valueDomainRepository)

        strategyCache = CacheBuilder.newBuilder().build(
            object : CacheLoader<Long, TransformPriorityStrategy>() {
                override fun load(jobId: Long): TransformPriorityStrategy {
                    val transformers = transformerRepository.loadTransformerWithJobId(jobId)
                    return DefaultTransformPriorityStrategy(transformers)
                }
            }
        )
    }

    override fun match(jobId: Long, itemName: String): Transformer {
        return strategyCache[jobId].match(jobId, itemName)
    }

    override fun match(jobId: Long, itemNames: Array<String>): Map<String, Transformer> {
        return strategyCache[jobId].match(jobId, itemNames)
    }

    internal class DPValueDomainRepository(private val repository: ValueDomainRepository) : ValueDomainRepository {
        override fun loadValueDomainWithStandardId(standardId: Long): ValueDomain {
            val valueDomain = repository.loadValueDomainWithStandardId(standardId)
            valueDomain.setSmartMapFlag(DPTransformConfig.INSTANCE.isValueDomainSmartMapOn)
            return valueDomain
        }

        override fun loadValueDomainAll(): List<ValueDomain> {
            val valueDomains = repository.loadValueDomainAll()
            for (valueDomain in valueDomains) {
                valueDomain.setSmartMapFlag(DPTransformConfig.INSTANCE.isValueDomainSmartMapOn)
            }

            return valueDomains
        }
    }

    companion object {

        val gateway: DPTransformGateway by lazy {
            // registry datasource for eig mc
            val resource = javaClass.getResourceAsStream("/source.properties")!!
            val sourceProp = Properties()
            sourceProp.load(resource)

            val mcDsConf = DataSourceConfig(
                name = "MC_DS",
                dbName = "eigmcdb",
                username = sourceProp.getProperty("eigmc.db.username"),
                password = sourceProp.getProperty("eigmc.db.password"),
                type = "mysql",
                host = sourceProp.getProperty("eigmc.db.hostname"),
                port = sourceProp.getProperty("eigmc.db.port").toInt()
            )

            val ctrlDsConf = DataSourceConfig(
                name = "CTRL_DS",
                dbName = "eigcontrol",
                username = sourceProp.getProperty("ctrl.db.username"),
                password = sourceProp.getProperty("ctrl.db.password"),
                type = "mysql",
                host = sourceProp.getProperty("ctrl.db.hostname"),
                port = sourceProp.getProperty("ctrl.db.port").toInt()
            )

            DatabaseSources.registryDataSource(mcDsConf)
            DatabaseSources.registryDataSource(ctrlDsConf)

            DPTransformGateway(
                DatabaseSources.getDataSource("MC_DS")!!,
                DatabaseSources.getDataSource("CTRL_DS")!!,
            )
        }
    }
}