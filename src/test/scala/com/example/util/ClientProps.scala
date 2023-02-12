package com.example.util

import com.typesafe.config.{ Config, ConfigFactory }
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.config.{ SaslConfigs, SslConfigs }

import java.net.URL
import java.util.Properties

case class ClientProps(
    bootstrapServer: String,
    apiKey: Option[String],
    apiSecret: Option[String],
    principal: Option[String]
) {

  val clientProps: Properties = new Properties()
  clientProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer)
  clientProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  apiKey zip apiSecret foreach {
    case (k, s) =>
      clientProps.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "https")
      clientProps.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL")
      clientProps.put(SaslConfigs.SASL_MECHANISM, "PLAIN")

      val saslString: String =
        s"""org.apache.kafka.common.security.plain.PlainLoginModule required username="${k}" password="${s}";""".stripMargin
      clientProps.setProperty(SaslConfigs.SASL_JAAS_CONFIG, saslString)
  }
}

case object ClientProps {

  val prefix = "kafka"

  def create(
      configFileUrl: Option[URL] = None,
      configPath: Option[String] = None
  ): ClientProps = {
    val topLevelConfig = configFileUrl.fold(ConfigFactory.load())(ConfigFactory.parseURL)
    val config = configPath.map(path => topLevelConfig.getConfig(path)).getOrElse(topLevelConfig)
    fromConfig(config)
  }

  def fromConfig(config: Config): ClientProps =
    ClientProps(
      config.getString(s"${prefix}.bootstrap"),
      if (config.hasPath(s"${prefix}.key")) Some(config.getString(s"${prefix}.key")) else None,
      if (config.hasPath(s"${prefix}.secret")) Some(config.getString(s"${prefix}.secret"))
      else None,
      if (config.hasPath(s"${prefix}.principal")) Some(config.getString(s"${prefix}.principal"))
      else None
    )
}
