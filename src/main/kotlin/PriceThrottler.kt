import kotlinx.coroutines.*

// Tests are included (in respective folder), gradle build is working
class PriceThrottler(private val scope: CoroutineScope) : PriceProcessor {
    private val subscribers: MutableList<PriceProcessor>
    private val ccyJobsMap: MutableMap<String, MutableList<Pair<Job, PriceProcessor>>>

    init {
        subscribers = ArrayList()
        ccyJobsMap = HashMap()
    }

    override fun onPrice(ccyPair: String?, rate: Double) {
        ccyPair?.let { ccy ->
            val jobList = ccyJobsMap.getOrPut(ccy) { ArrayList() }
            // It won't stop already running code UNLESS it can be stopped
            // (i.e. it's another coroutine that is currently suspended), which it is not
            jobList.onEach { pair -> pair.first.cancel() }.clear()
            jobList += subscribers.map { scope.launch { it.onPrice(ccy, rate) } to it }
        }
    }

    override fun subscribe(priceProcessor: PriceProcessor?) {
        priceProcessor?.let { subscribers += it }
    }

    override fun unsubscribe(priceProcessor: PriceProcessor?) {
        priceProcessor?.let {
            subscribers -= it
            ccyJobsMap.values.onEach { l ->
                l
                    .removeAll { pair ->
                        if (pair.second == it) {
                            pair.first.cancel()
                            true
                        } else {
                            false
                        }
                    }
            }
        }
    }
}
