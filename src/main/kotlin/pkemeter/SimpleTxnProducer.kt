package pkemeter

import com.typesafe.config.Config
import kotlinx.coroutines.InternalCoroutinesApi
import kotlinx.coroutines.NonCancellable.isActive
import kotlinx.coroutines.ObsoleteCoroutinesApi
import kotlinx.coroutines.channels.ticker
import kotlinx.coroutines.selects.select
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.KafkaException
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import pkemeter.CliHelper.recordFormat
import java.lang.invoke.MethodHandles
import java.util.concurrent.ExecutionException
import java.util.concurrent.Future

class SimpleTxnProducer(conf: Config) : TxnProducer {
    override lateinit var producer: KafkaProducer<String, String>
    override val kafka = KafkaConfiguration(conf)
    override val config = Configuration(conf)
    override val telemetry = PkeMetrics(conf, "simple-txn-producer")

    @ObsoleteCoroutinesApi
    @InternalCoroutinesApi
    override suspend fun run() {
        logger.info("run")

        // In this approach, the producer is created and transactions initialized only once and only a few
        // of the exceptions are handled.  The response to an exception is to abort the transaction, but try
        // to continue with the same producer and transaction session.
        // When a broker is restarted, there are transaction-related timeouts and exceptions that can occur and even
        // the abortTransaction() call will fail, causing the application to EXIT.
        //
        // See RobustTxnProducer.handleExceptions() for a more comprehensive list of exceptions.
        try {
            var numRecords = 0
            initProducer()
            val ticktock = ticker(delayMillis = (1000.0 / config.producerThroughput).toLong(), initialDelayMillis = 0)
            while (isActive) {
                select<Unit> {
                    ticktock.onReceive {
                        if (numRecords < config.producerNumberOfRecords) {
                            val key = makeKey()
                            val message = makeMessage(numRecords)
                            val record = ProducerRecord<String, String>(kafka.topics, key, message)

                            val result = sendRecord(record)?.get()
                            logger.info(String.format(recordFormat, result?.partition(), result?.offset(), message, numRecords))

                            ++numRecords
                        }
                    }
                }
            }
        } catch (e: Exception) {
            // We did not even get started or fatal error :(
            val jobWasCancelled = e.message?.contains("Job was cancelled")
            if (!jobWasCancelled!!) {
                logger.error("RUN-EX: $e, cause=${e.cause}")
            }
        }

        logger.warn("EXIT")
    }

    override fun sendRecord(record: ProducerRecord<String, String>): Future<RecordMetadata>? {
        telemetry.attempts()
        var result: Future<RecordMetadata>? = null
        try {
            producer.beginTransaction()

            // Not using callback per https://kafka.apache.org/23/javadoc/index.html?org/apache/kafka/clients/producer/KafkaProducer.html
            // The transactional producer uses exceptions to communicate error states. In particular, it is not required to
            // specify callbacks for producer.send() or to call .get() on the returned Future: a KafkaException would be
            // thrown if any of the producer.send() or transactional calls hit an irrecoverable error during a transaction.
            // See the send(ProducerRecord) documentation for more details about detecting errors from a transactional send.
            result = producer.send(record)

            //NB: the flush() is unnecessary since enable.idempotence=true will set retries Integer.MAX_VALUE and
            // acks=all (send won't complete until full set of in-sync replicas have ack'd).
            producer.flush()
            producer.commitTransaction()
            telemetry.successes()
        } catch (e: Exception) {
            telemetry.errors(e)
            handleExceptions(e)
        } finally {
            return result
        }
    }

    override fun handleExceptions(e: Exception) {
        when (e) {
            // See user code: https://gitlab.nordstrom.com/omni/order-pickup-tool/opt-kafka-client/blob/master/src/main/java/com/nordstrom/omni/opt_kafka/ProduceEvents.java#L250
            is KafkaException, is ExecutionException -> {
                producer.abortTransaction()
            }
        }
    }

    companion object {
        val logger: Logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass())
    }
} //-SimpleTxnProducer