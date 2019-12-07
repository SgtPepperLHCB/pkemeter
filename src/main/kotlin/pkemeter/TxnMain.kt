package pkemeter

import com.typesafe.config.ConfigFactory
import kotlinx.coroutines.*
import kotlinx.coroutines.selects.select
import kotlinx.serialization.UnstableDefault
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.lang.invoke.MethodHandles
import kotlin.system.exitProcess

class TxnMain {

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
            logger.debug("config:$config")

            // Start one of simple or robust producer based on application.conf
            var producer: TxnProducer = if (config.simpleTxnProducerEnabled) SimpleTxnProducer(conf) else RobustTxnProducer(conf)
//            // No-op for testing
//            val nop = NopTicker(conf)
            val consumer = SimpleTxnConsumer(conf)

            runBlocking {
                logger.info("CROSSING.THE.STREAMS")
                val jobs = mutableListOf<Job>(launch(Dispatchers.IO) { producer.run() })
                // Optionally start consumer
                if (config.consumerEnabled) jobs.add(launch(Dispatchers.IO) { consumer.run() })
//                jobs.add(launch(Dispatchers.IO) { nop.run() })

                // Wait for cancel signal
                select<Unit> {
                    cancel.onReceive {
                        logger.info("STREAMS.CROSSED")
                        jobs.forEach { it.cancelAndJoin() }
                        logger.info("PROTON.PACK.OVERLOAD")
                    }
                }
            } //-runBlocking
            logger.info("TOTAL.PROTONIC.REVERSAL")

            exitProcess(0)
        } //-main
    } //-companion object

} //-TxnMain
