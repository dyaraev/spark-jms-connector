package io.github.dyaraev.spark.connector.jms.common.client

import io.github.dyaraev.spark.connector.jms.common.ConnectionFactoryProvider
import io.github.dyaraev.spark.connector.jms.common.config.{CaseInsensitiveConfigMap, JmsConnectionConfig}
import jakarta.jms.ConnectionFactory

import java.util.Locale
import java.util.concurrent.ConcurrentHashMap

/**
 * Shared cache for JMS ConnectionFactory instances keyed by provider and broker options.
 */
object ConnectionFactoryCache {

  private case class FactoryCacheKey(provider: String, brokerOptions: CaseInsensitiveConfigMap)

  private val cfCache = new ConcurrentHashMap[FactoryCacheKey, ConnectionFactory]()

  /**
   * Resolve a JMS ConnectionFactory from cache or construct and cache it.
   */
  def getOrCreate(config: JmsConnectionConfig): ConnectionFactory = {
    val key = FactoryCacheKey(
      config.provider.toLowerCase(Locale.ROOT),
      config.brokerOptions,
    )

    val cached = cfCache.get(key)
    if (cached != null) {
      cached
    } else {
      val provider = ConnectionFactoryProvider.createInstanceByBrokerName(config.provider)
      val created = provider.getConnectionFactory(config.brokerOptions)
      val previous = cfCache.putIfAbsent(key, created)
      if (previous != null) previous else created
    }
  }
}
