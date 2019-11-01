package com.provectus.kafka.dynamodb

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kafka.connect.sink.SinkTask

class DynamoDbSinkTask : SinkTask() {
    private lateinit var dao: DynamoDAO
    private lateinit var config: DynamoDbSinkConfig

    override fun start(props: Map<String, String>?) {
        DynamoDbSinkConnectorConfig(props ?: emptyMap()).also { connectorConfig ->
            dao = DynamoDAO(connectorConfig.dynamoDbTableName, AmazonDynamoDBClientBuilder.defaultClient())
            config = connectorConfig.config
        }
    }

    override fun put(records: Collection<SinkRecord>?) {
        if (records == null)
            return

        records.asSequence().map {
            config.keyExtractor(it) to config.valueExtractor(it)
        }.filter {
            it.first != null && it.second != null
        }.map {
            it.first!! to it.second!!
        }.groupBy({ it.first }) {
            it.second
        }.mapValues { (key, values) ->
            values.reduce { left, right ->
                config.reduce(key, left, right)
            }
        }.also { data ->
            config.merge(data.values, dao.batchGet(data.keys)).also {
                dao.batchWrite(it)
            }
        }
    }

    override fun stop() = Unit
    override fun version() = version
}