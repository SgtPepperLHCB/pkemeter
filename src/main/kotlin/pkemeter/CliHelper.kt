package pkemeter

import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.launch
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import sun.misc.Signal
import java.lang.invoke.MethodHandles

object CliHelper {
    val logger: Logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass())

    fun trapSignal(signal: String): ReceiveChannel<Unit> {
        val channel = Channel<Unit>()

        Signal.handle(Signal(signal)) { sig ->
            logger.warn("signaled:$sig")
            GlobalScope.launch {
                channel.send(Unit)
            }
        }

        return channel
    }

    val charPool: List<Char> = ('a'..'z') + ('A'..'Z') + ('0'..'9')
    fun messageString(size: Long): String {
        return (1..size)
                .map { kotlin.random.Random.nextInt(0, charPool.size) }
                .map(TxnMain.charPool::get)
                .joinToString("")
    }

    const val recordFormat = "p=%3d, o=%3d, m=%21s, n=%5d"

} //-CliHelper