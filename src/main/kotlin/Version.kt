package com.provectus.kafka.dynamodb

internal val version: String
    get() = try {
        DynamoDbSinkConnector::class.java.`package`.implementationVersion
    } catch (e: Exception) {
        "0.0.0.0"
    }
