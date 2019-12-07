package pkemeter

import com.typesafe.config.Config
import java.util.*

//
// Converts Config to kafka consumer/producer configuration properties
//
class KafkaConfiguration(val config: Config) {
    val topics = config.getString("kafka.topics")
    val poll_ms = config.getLong("kafka.poll.ms")

    val consumerMap = makeMap("kafka.consumer")
    val txnConsumerMap = makeMap("kafka.txn-consumer")
    val producerMap = makeMap("kafka.producer")
    val txnProducerMap = makeMap("kafka.txn-producer")

    val kafkaMap = makeKafkaMap("kafka.servers")
    private fun makeKafkaMap(node: String): MutableMap<String, String> {
        val map = makeMap(node)
        // Remove username/password but use values to create sasl.jaas.config
        if ("sasl.username" in map) {
            val user = map["sasl.username"]
            val pass = map["sasl.password"]
            map.remove("sasl.username")
            map.remove("sasl.password")
            map["sasl.jaas.config"] =
                    """
org.apache.kafka.common.security.scram.ScramLoginModule required username="$user" password="$pass";
                """.trimIndent()
        }

        return map
    }

    private fun makeMap(configField: String): MutableMap<String, String> {
        val map = mutableMapOf<String, String>()
        config.getConfig(configField).entrySet().forEach {
            val k: String = it.key
            val v: String = it.value.unwrapped().toString()
            map[k] = v
        }
        return map
    }

    fun consumerProperties(): Properties {
        // kafka + consumer
        val props = kafkaMap.toProperties()
        props += consumerMap.toProperties()
        return props
    }

    fun txnConsumerProperties(): Properties {
        // kafka + consumer
        val props = kafkaMap.toProperties()
        props += consumerMap.toProperties()
        props += txnConsumerMap.toProperties()
        return props
    }

    fun producerProperties(): Properties {
        // kafka + producer
        val props = kafkaMap.toProperties()
        props += producerMap.toProperties()
        return props
    }

    fun txnProducerProperties(): Properties {
        // kafka + producer
        val props = kafkaMap.toProperties()
        props += producerMap.toProperties()
        props += txnProducerMap.toProperties()
        return props
    }

} //-KafkaConfiguration
