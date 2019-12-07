package pkemeter

import com.typesafe.config.Config
import io.micrometer.core.instrument.Clock
import io.micrometer.core.instrument.Metrics
import io.micrometer.core.instrument.config.NamingConvention
import io.micrometer.jmx.JmxConfig
import io.micrometer.jmx.JmxMeterRegistry
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.errors.AuthorizationException
import org.apache.kafka.common.errors.OutOfOrderSequenceException
import org.apache.kafka.common.errors.ProducerFencedException
import org.apache.kafka.common.errors.TimeoutException
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.lang.invoke.MethodHandles
import java.time.Duration
import java.util.concurrent.ExecutionException

class PkeMetrics(val conf: Config, val prefix: String) {

    init {
        val registry = JmxMeterRegistry(object : JmxConfig {
            override fun step(): Duration {
                return Duration.ofSeconds(10)
            }

            override fun get(k: String): String? {
                return null
            }

            override fun domain(): String {
                return "pkemeter-$prefix"
            }
        }, Clock.SYSTEM)
        registry.config().namingConvention(NamingConvention.dot)
        Metrics.addRegistry(registry)

    }

    val attempt = Metrics.globalRegistry.counter("$prefix.attempt.count")
    val error = Metrics.globalRegistry.counter("$prefix.error.count")
    val success = Metrics.globalRegistry.counter("$prefix.success.count")

    val exception = Metrics.globalRegistry.counter("$prefix.error.Exception.count")
    val authorizationException = Metrics.globalRegistry.counter("$prefix.error.AuthorizationException.count")
    val executionException = Metrics.globalRegistry.counter("$prefix.error.ExecutionException.count")
    val kafkaException = Metrics.globalRegistry.counter("$prefix.error.KafkaException.count")
    val outOfOrderSequenceException = Metrics.globalRegistry.counter("$prefix.error.OutOfOrderSequenceException.count")
    val producerFencedException = Metrics.globalRegistry.counter("$prefix.error.ProducerFencedException.count")
    val timeoutException = Metrics.globalRegistry.counter("$prefix.error.TimeoutException.count")
    val notTracked = Metrics.globalRegistry.counter("$prefix.error.exception-not-tracked.count")

    fun attempts() {
        attempt.increment()
    }

    fun errors(e: Exception) {
        error.increment()
        handleExceptions(e)
    }

    fun successes() {
        success.increment()
    }

    fun handleExceptions(e: Exception) {
        // Make sure we attribute the cause
        when (if (e.cause != null) e.cause else e) {
            is AuthorizationException -> authorizationException.increment()
            is OutOfOrderSequenceException -> outOfOrderSequenceException.increment()
            is ProducerFencedException -> producerFencedException.increment()
            is KafkaException -> kafkaException.increment()
            is TimeoutException -> timeoutException.increment()
            is ExecutionException -> executionException.increment()
            else -> notTracked.increment()
        }
    }

    companion object {
        val logger: Logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass())
    }
} //-PkeMetrics