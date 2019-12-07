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
import org.apache.kafka.common.errors.AuthorizationException
import org.apache.kafka.common.errors.OutOfOrderSequenceException
import org.apache.kafka.common.errors.ProducerFencedException
import org.apache.kafka.common.errors.UnsupportedVersionException
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import pkemeter.CliHelper.recordFormat
import java.lang.invoke.MethodHandles
import java.util.concurrent.ExecutionException
import java.util.concurrent.Future

class RobustTxnProducer(conf: Config) : TxnProducer {
    override lateinit var producer: KafkaProducer<String, String>
    override val kafka = KafkaConfiguration(conf)
    override val config = Configuration(conf)
    override val telemetry = PkeMetrics(conf, "robust-txn-producer")

    @ObsoleteCoroutinesApi
    @InternalCoroutinesApi
    override suspend fun run() {
        logger.info("run")

        // In this approach, exceptions are capture and an attempt is made to first abort the transaction and,
        // if that fails, then attempt to recover by creating a new KafkaProducer (using the same
        // transactional.id) and initializing the transaction coordinator.

        //NB: catch Exception will catch all checked and runtime exceptions.
        // We do not catch Throwable since it presents *all* exceptions and could be a result of some systemic
        // problem (e.g., OutOfMemoryException) for which there is no recovery.
        var numRecords = 0
        try {
            val ticktock = ticker(delayMillis = (1000.0 / config.producerThroughput).toLong(), initialDelayMillis = 0)
            initProducer()
            while (isActive) {
                try {
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
                    } //-select
                } catch (e: Exception) {
                    handleExceptions(e)
                }
            } //-while
        } catch (e: Exception) {
            // We may not have even started or fatal error :(
            handleCoroutineJobCancel(e)
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

            producer.flush()
            producer.commitTransaction()

            telemetry.successes()
        } catch (e: Exception) {
            handleExceptions(e)
        } finally {
            return result
        }
    }

    override fun handleExceptions(e: Exception) {
        telemetry.errors(e)
        // See https://kafka.apache.org/23/javadoc/org/apache/kafka/clients/producer/KafkaProducer.html#send-org.apache.kafka.clients.producer.ProducerRecord-
        when (e) {
            //TODO AuthenticationException
            //TODO IllegalStateException
            //TODO InterruptException
            //TODO SerializationException
            //TODO _Usually_, ExecutionException.cause == TimeoutException
            is ProducerFencedException, is OutOfOrderSequenceException, is AuthorizationException, is UnsupportedVersionException -> {
                // Cannot recover from these exceptions, so only option is to close producer and start over.
                logger.error("KAF-EX0: $e")
                autoRecover()
            }
            is KafkaException -> {
                // For all other Kafka exceptions, abort the transaction and try again without restarting producer
                logger.error("KAF-EX1: $e")
                //NB: abortTransaction() can throw:
                //  IllegalStateException - if no transactional.id has been configured or no transaction has been started
                //  ProducerFencedException - fatal error indicating another producer with the same transactional.id is active
                //  UnsupportedVersionException - fatal error indicating the broker does not support transactions (i.e. if its version is lower than 0.11.0.0)
                //  AuthorizationException - fatal error indicating that the configured transactional.id is not authorized. See the exception for more details
                //  KafkaException - if the producer has encountered a previous fatal error or for any other unexpected error
                //  TimeoutException - if the time taken for aborting the transaction has surpassed max.block.ms.
                //  InterruptException - if the thread is interrupted while blocked
                // See https://kafka.apache.org/23/javadoc/org/apache/kafka/clients/producer/KafkaProducer.html#abortTransaction--
                try {
                    producer.abortTransaction()
                } catch (ee: Exception) {
                    autoRecover()
                }
            }
            is ExecutionException -> {
                // Cannot recover from these exceptions, which may occur during a transaction function (e.g.,
                // initTransactions(), beginTransaction(), commitTransaction(), abortTransaction() when transaction
                // coordinator (broker) is temporarily offline.
                // Only option is to close producer and start over.
                logger.error("KAF-EX2: $e")
                autoRecover()
            }
            // All other exceptions are fatal (i.e., non-Kafka exceptions).
            else -> {
                handleCoroutineJobCancel(e)
                throw e
            }
        }
    }

    private fun autoRecover() {
        logger.warn("attempting AUTO-RECOVER")
        //NB: close() can throw InterruptException
        // See https://kafka.apache.org/23/javadoc/org/apache/kafka/clients/producer/KafkaProducer.html#close--
        producer.close()
        initProducer()
    }

    private fun handleCoroutineJobCancel(e: Exception) {
        val jobWasCancelled = e.message?.contains("Job was cancelled")
        if (!jobWasCancelled!!) {
            logger.error("RUN-EXA - UNRECOVERABLE: $e")
        }
    }

    companion object {
        val logger: Logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass())
    }
} //-RobustTxnProducer