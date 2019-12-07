package pkemeter

import com.typesafe.config.ConfigFactory
import kotlinx.coroutines.InternalCoroutinesApi
import kotlinx.coroutines.ObsoleteCoroutinesApi
import kotlinx.coroutines.channels.ticker
import kotlinx.coroutines.isActive
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.selects.select
import kotlinx.serialization.UnstableDefault
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import pkemeter.CliHelper.recordFormat
import java.lang.invoke.MethodHandles
import kotlin.system.exitProcess

class SimpleMain {
    companion object {
        val logger: Logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass())
        val charPool: List<Char> = ('a'..'z') + ('A'..'Z') + ('0'..'9')

        @ObsoleteCoroutinesApi
        @JvmStatic
        @InternalCoroutinesApi
        @UnstableDefault
        fun main(args: Array<String>) {
            val cancel = CliHelper.trapSignal("INT")

            val conf = ConfigFactory.load()
            val config = Configuration(conf)
            val kafka = KafkaConfiguration(conf)

            val telemetry = PkeMetrics(conf, "simple")

            var numRecords = 0
            try {
                val producer = KafkaProducer<String, String>(kafka.producerProperties())
                val ticktock = ticker(delayMillis = (1000.0 / config.producerThroughput).toLong(), initialDelayMillis = 0)
                runBlocking {
                    logger.info("CROSSING.THE.STREAMS")
                    // Wait for either ticker or cancel signal via ctrl+c
                    while (isActive) {
                        select<Unit> {
                            cancel.onReceive {
                                logger.info("STREAMS.CROSSED")
                                ticktock.cancel()
                                producer.close()
                                logger.info("PROTON.PACK.OVERLOAD")
                            }
                            ticktock.onReceive {
                                if (numRecords < config.producerNumberOfRecords) {
                                    val message = messageString(config.producerRecordSizeBytes)
                                    val record = ProducerRecord<String, String>(kafka.topics, message)
                                    try {
                                        telemetry.attempts()

                                        val result = producer.send(record).get()

                                        ++numRecords
                                        logger.debug("send.OK: ${String.format(recordFormat, result?.partition(), result?.offset(), message, numRecords)}")
                                        telemetry.successes()
                                    } catch (e: Exception) {
                                        telemetry.errors(e)
                                        logger.error("KAFKA-ERROR: $e")
                                    }
                                }
                            }
                        }
                    }
                }
            } catch (e: Exception) {
                // Nothing to do but exit
                logger.debug("E: $e")
            }

            logger.trace("produced $numRecords records")
            logger.info("TOTAL.PROTONIC.REVERSAL")
            exitProcess(0)
        } //-main

        private fun messageString(size: Long): String {
            return (1..size)
                    .map { kotlin.random.Random.nextInt(0, charPool.size) }
                    .map(charPool::get)
                    .joinToString("")
        }
    } //-companion object

} //-SimpleMain
