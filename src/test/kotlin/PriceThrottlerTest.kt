import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.asCoroutineDispatcher
import org.assertj.core.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.concurrent.Executors

class PriceThrottlerTest {
    private var subscriberList: MutableList<SlowSubscriber>? = null
    private var priceThrottler: PriceThrottler? = null

    @BeforeEach
    fun setUp() {
        priceThrottler = PriceThrottler(
            CoroutineScope(Executors.newSingleThreadExecutor().asCoroutineDispatcher())
        )
        subscriberList = ArrayList()
        repeat(5) {
            subscriberList!! += SlowSubscriber()
        }
    }

    @Test
    fun onPriceNormal() {
        for (s in subscriberList!!) {
            priceThrottler!!.subscribe(s)
        }

        priceThrottler!!.onPrice("EURUSD", 1.02)
        Thread.sleep(550)
        assertThat(subscriberList)
            .allSatisfy { assertThat(it.wasExecuted).isEqualTo(true) }
            .allSatisfy { assertThat(it.hasFinished).isEqualTo(true) }
            .allSatisfy { assertThat(it.ccyHistory).containsOnly("EURUSD") }
            .allSatisfy { assertThat(it.rateHistory).containsOnly(1.02) }
    }

    @Test
    fun onPriceThrottle() {
        for (s in subscriberList!!) {
            priceThrottler!!.subscribe(s)
        }

        priceThrottler!!.onPrice("EURUSD", 1.02)
        priceThrottler!!.onPrice("EURUSD", 1.08)
        priceThrottler!!.onPrice("RUBJPY", 0.72)
        priceThrottler!!.onPrice("EURUSD", 1.05)
        priceThrottler!!.onPrice("EURUSD", 1.11)

        Thread.sleep(1500)
        assertThat(subscriberList)
            .allSatisfy { assertThat(it.wasExecuted).isEqualTo(true) }
            .allSatisfy { assertThat(it.hasFinished).isEqualTo(true) }
            .allSatisfy { assertThat(it.ccyHistory).contains("EURUSD", "RUBJPY") }
            .allSatisfy { assertThat(it.rateHistory).doesNotContain(1.08, 1.05) }
    }

    @Test
    fun onPriceUnsubscribe() {
        for (s in subscriberList!!) {
            priceThrottler!!.subscribe(s)
        }

        priceThrottler!!.onPrice("EURUSD", 1.02)
        Thread.sleep(600)
        priceThrottler!!.unsubscribe(subscriberList!![0])
        priceThrottler!!.unsubscribe(subscriberList!![1])
        priceThrottler!!.onPrice("RUBJPY", 0.72)

        Thread.sleep(1000)
        assertThat(subscriberList)
            .allSatisfy { assertThat(it.wasExecuted).isEqualTo(true) }
            .allSatisfy { assertThat(it.hasFinished).isEqualTo(true) }
            .satisfies({ assertThat(it.ccyHistory).doesNotContain("RUBJPY") }, atIndex(0))
            .satisfies({ assertThat(it.ccyHistory).doesNotContain("RUBJPY") }, atIndex(1))
    }
}

private class SlowSubscriber : PriceProcessor {
    var wasExecuted = false
    var hasFinished = false
    val ccyHistory = mutableListOf<String>()
    val rateHistory = mutableListOf<Double>()

    override fun onPrice(ccyPair: String?, rate: Double) {
        wasExecuted = true
        ccyHistory += ccyPair!!
        rateHistory += rate
        Thread.sleep(100)
        hasFinished = true
    }

    override fun subscribe(priceProcessor: PriceProcessor?) {
        TODO("Unreachable")
    }

    override fun unsubscribe(priceProcessor: PriceProcessor?) {
        TODO("Unreachable")
    }
}