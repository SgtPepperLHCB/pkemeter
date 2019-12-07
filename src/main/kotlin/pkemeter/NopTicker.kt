package pkemeter

import com.typesafe.config.Config
import kotlinx.coroutines.InternalCoroutinesApi
import kotlinx.coroutines.NonCancellable.isActive
import kotlinx.coroutines.ObsoleteCoroutinesApi
import kotlinx.coroutines.channels.ticker
import kotlinx.coroutines.selects.select
import org.apache.kafka.clients.producer.KafkaProducer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.lang.invoke.MethodHandles

class NopTicker(conf: Config) {
    val kafka = KafkaConfiguration(conf)
    val config = Configuration(conf)
    val telemetry = PkeMetrics(conf, "nop")

    @ObsoleteCoroutinesApi
    @InternalCoroutinesApi
    suspend fun run() {
        logger.info("run")

        val ticktock = ticker(delayMillis = (1000.0 / config.producerThroughput).toLong(), initialDelayMillis = 0)
        var tick = false
        while (isActive) {
            select<Unit> {
                ticktock.onReceive {
                    telemetry.attempts()

                    logger.info(if (tick) "tick" else "tock")
                    tick = !tick

                    telemetry.successes()
                } //-ticktock
            } //-select
        } //-while

        logger.info("EXIT")
    }

    companion object {
        val logger: Logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass())
    }
}