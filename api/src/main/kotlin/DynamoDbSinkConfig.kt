package com.provectus.kafka.dynamodb

import com.amazonaws.services.dynamodbv2.document.Item
import com.amazonaws.services.dynamodbv2.document.PrimaryKey
import org.apache.kafka.connect.sink.SinkRecord

typealias KeyExtractor = SinkRecord.() -> PrimaryKey?
typealias ValueExtractor = SinkRecord.() -> Item?
typealias ReduceFunction = (key: PrimaryKey, left: Item, right: Item) -> Item
typealias MergeFunction = (newItems: Collection<Item>, existingItems: Collection<Item>) -> Collection<Item>

interface DynamoDbSinkConfig {
    val keyExtractor: KeyExtractor
    val valueExtractor: ValueExtractor
    val reduce: ReduceFunction
    val merge: MergeFunction
}
