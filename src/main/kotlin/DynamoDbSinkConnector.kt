package com.provectus.kafka.dynamodb

import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.sink.SinkConnector

class DynamoDbSinkConnector : SinkConnector() {
    private lateinit var configProperties: Map<String, String>

    override fun start(props: Map<String, String>?) {
        try {
            configProperties = props ?: emptyMap()
            DynamoDbSinkConnectorConfig(configProperties)
        } catch (e: ConfigException) {
            throw ConnectException("Couldn't start DynamoDbSinkConnector due to configuration error", e)
        }
    }

    override fun taskConfigs(maxTasks: Int) = Array(maxTasks) { configProperties.toMap() }.toList()

    override fun stop() = Unit

    override fun version() = version

    override fun taskClass() = DynamoDbSinkTask::class.java

    override fun config() = DynamoDbSinkConnectorConfig.config
}