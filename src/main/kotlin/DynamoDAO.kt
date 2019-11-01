package com.provectus.kafka.dynamodb

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.document.*
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemResult

class DynamoDAO(private val tableName: String, amazonDynamoDB: AmazonDynamoDB) {
    private val client = DynamoDB(amazonDynamoDB)
    private val table = client.getTable(this.tableName)

    fun batchGet(keys: Collection<PrimaryKey>): List<Item> {
        val result = client.batchGetItem(
                TableKeysAndAttributes(tableName).withPrimaryKeys(*keys.toTypedArray())
        )
        return result.tableItems[tableName] ?: emptyList()
    }

    fun batchWrite(items: Collection<Item>): BatchWriteItemResult {
        val threadTableWriteItems = TableWriteItems(tableName).withItemsToPut(items)
        val result = client.batchWriteItem(threadTableWriteItems)
        return result.batchWriteItemResult
    }
}