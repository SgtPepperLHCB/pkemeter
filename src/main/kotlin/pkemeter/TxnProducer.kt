package pkemeter

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.lang.invoke.MethodHandles
import java.util.concurrent.Future

interface TxnProducer {
    var producer: KafkaProducer<String, String>
    val kafka: KafkaConfiguration
    val config: Configuration
    val telemetry: PkeMetrics

    // This is where the magic happens
    suspend fun run()

    fun sendRecord(record: ProducerRecord<String, String>): Future<RecordMetadata>? {
        return null
    }

    fun handleExceptions(e: Exception) {
    }

    fun initProducer() {
        logger.trace(".init-producer")
        producer = KafkaProducer(kafka.txnProducerProperties())
        //TODO: initTransaction() can throw:
        //  IllegalStateException - if no transactional.id has been configured
        //  UnsupportedVersionException - fatal error indicating the broker does not support transactions (i.e. if its version is lower than 0.11.0.0)
        //  AuthorizationException - fatal error indicating that the configured transactional.id is not authorized. See the exception for more details
        //  KafkaException - if the producer has encountered a previous fatal error or for any other unexpected error
        //  TimeoutException - if the time taken for initialize the transaction has surpassed max.block.ms.
        //  InterruptException - if the thread is interrupted while blocked
        logger.trace(".init-producer.init-transactions")
        producer.initTransactions()
        logger.trace(".init-producer.init-transactions.OK")
    }

    fun makeKey(): String {
        return "${kotlin.random.Random.nextLong()}"
    }

    fun makeMessage(nnn: Int): String {
        return "$nnn:${CliHelper.messageString(config.producerRecordSizeBytes)}"
    }

    companion object {
        val logger: Logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass())
//        const val recordFormat = "p=%3d, o=%3d, m=%21s, n=%5d"
    }
}