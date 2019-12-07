package pkemeter

import com.typesafe.config.Config
import kotlinx.coroutines.InternalCoroutinesApi
import kotlinx.coroutines.NonCancellable.isActive
import kotlinx.coroutines.yield
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import pkemeter.CliHelper.recordFormat
import java.lang.invoke.MethodHandles
import java.time.Duration

class SimpleTxnConsumer(conf: Config) {
    lateinit var consumer: KafkaConsumer<String, String>
    val kafka = KafkaConfiguration(conf)
    val config = Configuration(conf)
//    val telemetry = PkeMetrics(conf, "txn-consumer")

    @InternalCoroutinesApi
    suspend fun run() {
        logger.info("run, txn=${config.txnConsumerEnabled}")

        // A consumer reading transaction messages should set isolation.level = read_committed
        consumer = KafkaConsumer(if (config.txnConsumerEnabled) kafka.txnConsumerProperties() else kafka.consumerProperties())
        consumer.subscribe(listOf(kafka.topics))

        var numRecords = 0
        while (isActive) {
            logger.trace(".consumer.poll: ${kafka.poll_ms}ms")
            val records = consumer.poll(Duration.ofMillis(kafka.poll_ms))
            records.forEach {
                ++numRecords
                logger.info(String.format(recordFormat, it.partition(), it.offset(), it.value(), numRecords))
            }
            yield()
        }

        logger.info("EXIT")
    }

    companion object {
        val logger: Logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass())
    }

} //-SimpleTxnConsumer