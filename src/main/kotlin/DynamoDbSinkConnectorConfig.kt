package com.provectus.kafka.dynamodb

import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.common.config.ConfigDef
import org.slf4j.LoggerFactory

class DynamoDbSinkConnectorConfig(parsedConfig: Map<String, String>) : AbstractConfig(config, parsedConfig) {
    init {
        LoggerFactory.getLogger(javaClass).also {
            if (it.isDebugEnabled) {
                it.debug("""DynamoDB Sink configurations:
                    |   $DYNAMO_TABLE_NAME: ${parsedConfig[DYNAMO_TABLE_NAME] ?: "!!!Error: value is null!!!"}
                    |   $CONFIG_CLASS_NAME: ${parsedConfig[CONFIG_CLASS_NAME] ?: "!!!Error: value is null!!!"}
                    """.trimMargin()
                )
            }
        }
    }

    internal companion object {
        val config: ConfigDef = ConfigDef().apply {
            define(DYNAMO_TABLE_NAME, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, DYNAMO_TABLE_DOC)
            define(CONFIG_CLASS_NAME, ConfigDef.Type.CLASS, ConfigDef.Importance.HIGH, CONFIG_CLASS_DOC)
        }

        private const val DYNAMO_TABLE_NAME = "dynamo.table"
        private const val DYNAMO_TABLE_DOC = "Table name of the DynamoDB you want to work on."

        private const val CONFIG_CLASS_NAME = "config.class"
        private const val CONFIG_CLASS_DOC = "Class name (should implement interface " +
                "com.provectus.kafka.dynamodb.DynamoDbSinkConfig) to configure Sink logic"
    }

    val dynamoDbTableName: String by lazy {
        getString(DYNAMO_TABLE_NAME)
    }

    val config: DynamoDbSinkConfig by lazy {
        val target = getClass(CONFIG_CLASS_NAME).getDeclaredConstructor().newInstance() as DynamoDbSinkConfig
        object : DynamoDbSinkConfig {
            override val keyExtractor by lazy { target.keyExtractor }
            override val valueExtractor by lazy { target.valueExtractor }
            override val reduce by lazy { target.reduce }
            override val merge by lazy { target.merge }
        }
    }
}