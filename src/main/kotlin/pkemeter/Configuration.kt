package pkemeter

import com.typesafe.config.Config

class Configuration(val config: Config) {
    val consumerEnabled = config.getBoolean("application.consumer.enable")
    val txnConsumerEnabled = config.getBoolean("application.consumer.txn")
    val simpleTxnProducerEnabled = config.getBoolean("application.producer.simple_txn")
    val producerNumberOfRecords = config.getLong("application.producer.num_records")
    val producerThroughput = config.getDouble("application.producer.throughput")
    val producerRecordSizeBytes = config.getLong("application.producer.record_size_bytes")

    override fun toString(): String {
        return """
            num_records=$producerNumberOfRecords,
            throughput=$producerThroughput,
            record_size_bytes=$producerRecordSizeBytes,
            simple_txn=$simpleTxnProducerEnabled,
            consumer_enabled=$consumerEnabled,
            consumer_txn=$txnConsumerEnabled
        """.trimIndent()
    }
}