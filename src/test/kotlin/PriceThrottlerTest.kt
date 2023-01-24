import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.asCoroutineDispatcher
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.atIndex
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.concurrent.Executors

/**
 * Test you sent me is "onPriceThrottleHard"
 */
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
    fun onPriceThrottleSimple() {
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

    @Test
    fun onPriceThrottleManyCcys() {
        for (s in subscriberList!!) {
            priceThrottler!!.subscribe(s)
        }

        var cnt = 0
        priceThrottler!!.onPrice("AAABBB", (++cnt).toDouble())
        priceThrottler!!.onPrice("CCCDDD", (++cnt).toDouble())
        priceThrottler!!.onPrice("EEEFFF", (++cnt).toDouble())
        priceThrottler!!.onPrice("GGGHHH", (++cnt).toDouble())
        priceThrottler!!.onPrice("ZZZYYY", (++cnt).toDouble())
        priceThrottler!!.onPrice("ZZZYYY", (++cnt).toDouble())
        priceThrottler!!.onPrice("ZZZYYY", (++cnt).toDouble())
        priceThrottler!!.onPrice("ZZZYYY", (++cnt).toDouble())
        priceThrottler!!.onPrice("XXX@@@", (++cnt).toDouble())
        priceThrottler!!.onPrice("SSS!!!", (++cnt).toDouble())
        priceThrottler!!.onPrice("JJJ###", (++cnt).toDouble())

        Thread.sleep(5000)

        assertThat(subscriberList)
            .allSatisfy {
                assertThat(it.ccyHistory).contains(
                    "AAABBB", "CCCDDD", "EEEFFF", "GGGHHH", "ZZZYYY", "XXX@@@", "SSS!!!", "JJJ###"
                )
            }
    }

    @Test
    fun onPriceThrottleHardSimplified() {
        for (s in subscriberList!!) {
            priceThrottler!!.subscribe(s)
        }

        var cnt = 0
        priceThrottler!!.onPrice("EURUSD", cnt++.toDouble()) // cnt will become 1
        priceThrottler!!.onPrice("EURUSD", cnt++.toDouble()) // 2
        priceThrottler!!.onPrice("EURUSD", cnt++.toDouble()) // 3
        priceThrottler!!.onPrice("EURUSD", cnt++.toDouble()) // 4
        priceThrottler!!.onPrice("EURUSD", cnt++.toDouble()) // 5
        priceThrottler!!.onPrice("EURUSD", cnt++.toDouble()) // 6
        priceThrottler!!.onPrice("DDDFFF", cnt++.toDouble()) // 7
        priceThrottler!!.onPrice("EURUSD", cnt++.toDouble()) // 8
        priceThrottler!!.onPrice("EURUSD", cnt++.toDouble()) // 9
        priceThrottler!!.onPrice("EURUSD", cnt++.toDouble()) // 10
        priceThrottler!!.onPrice("EURUSD", cnt.toDouble()) // So, 10

        Thread.sleep(2000)
        assertThat(subscriberList)
            .allSatisfy { assertThat(it.wasExecuted).isEqualTo(true) }
            .allSatisfy { assertThat(it.hasFinished).isEqualTo(true) }
            .allSatisfy { assertThat(it.ccyHistory).contains("EURUSD", "DDDFFF") }
            .allSatisfy { assertThat(it.rateHistory).contains(0.0, 6.0, 10.0) }
    }

    @Test
    fun onPriceThrottleHard() {
        subscriberList = ArrayList()
        subscriberList!! += SlowSubscriber(100)
        subscriberList!! += SlowSubscriber(10)
        subscriberList!! += SlowSubscriber(1000)
        for (s in subscriberList!!) {
            priceThrottler!!.subscribe(s)
        }

        var r = 0
        while (r < 30) {
            priceThrottler!!.onPrice("ARSRUB", r++.toDouble())

            if (r % 10 == 0) {
                priceThrottler!!.onPrice("DDDFFF", r++.toDouble())
            }

            Thread.sleep(10)
        }

        Thread.sleep(5000)
        assertThat(subscriberList)
            .allSatisfy { assertThat(it.wasExecuted).isEqualTo(true) }
            .allSatisfy { assertThat(it.hasFinished).isEqualTo(true) }
            .allSatisfy { assertThat(it.ccyHistory).contains("ARSRUB", "DDDFFF") }
            .allSatisfy { assertThat(it.rateHistory).contains(0.0, 10.0, 30.0) }
    }

    @Test
    fun unsubscribe() {
        for (s in subscriberList!!) {
            priceThrottler!!.subscribe(s)
        }

        priceThrottler!!.unsubscribe(subscriberList!![0])
        priceThrottler!!.unsubscribe(subscriberList!![1])
        priceThrottler!!.onPrice("EURUSD", 1.02)

        Thread.sleep(600)
        assertThat(subscriberList)
            .satisfies({ assertThat(it.wasExecuted).isEqualTo(false) }, atIndex(0))
            .satisfies({ assertThat(it.wasExecuted).isEqualTo(false) }, atIndex(1))
    }
}

private class SlowSubscriber(val sleep: Long) : PriceProcessor {
    var wasExecuted = false
    var hasFinished = false
    val ccyHistory = mutableListOf<String>()
    val rateHistory = mutableListOf<Double>()

    constructor() : this(100)

    override fun onPrice(ccyPair: String?, rate: Double) {
        wasExecuted = true
        ccyHistory += ccyPair!!
        rateHistory += rate
        Thread.sleep(sleep)
        println("I am $this, got: $ccyPair:$rate")
        hasFinished = true
    }

    override fun subscribe(priceProcessor: PriceProcessor?) {
        TODO("Unreachable")
    }

    override fun unsubscribe(priceProcessor: PriceProcessor?) {
        TODO("Unreachable")
    }

    override fun toString(): String =
        "SlowSubscriber[" +
                this.sleep.toString().padEnd(4, ' ') +
                "]@" +
                this.hashCode().toString(16).padStart(8, '0')
}
