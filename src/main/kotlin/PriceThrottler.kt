import kotlinx.coroutines.*

private typealias CurrentAndNextJobs = PriceThrottler.MutablePair<Job?, Job?>
private typealias ProcessorJobsPair = Pair<PriceProcessor, CurrentAndNextJobs>

// Tests are included (in respective folder) (including new one), gradle build is working
class PriceThrottler(private val scope: CoroutineScope) : PriceProcessor {
    private val subscribers: MutableList<PriceProcessor>

    private val ccyProcessorJobsPairs: MutableMap<String, MutableList<ProcessorJobsPair>>

    data class MutablePair<A, B>(var first: A, var second: B)

    init {
        subscribers = ArrayList()
        ccyProcessorJobsPairs = HashMap()
    }

    // For testing purposes
    constructor() : this(CoroutineScope(Dispatchers.Default + SupervisorJob()))

    override fun onPrice(ccyPair: String?, rate: Double) {
        ccyPair?.let { ccy ->
            subscribers.onEach { delegateJobsProcessing(it, ccy, rate) }
        }
    }

    private fun nextJob(
        jobsPair: CurrentAndNextJobs, sub: PriceProcessor, ccy: String, rate: Double,
        callbackFromJob: Boolean = false
    ) {
        if (callbackFromJob) {

            jobsPair.first = jobsPair.second
            jobsPair.second = null // In case it wasn't null
            jobsPair.first?.start()

        } else if (jobsPair.first == null && jobsPair.second == null) {

            jobsPair.first = scope.launch {
                sub.onPrice(ccy, rate)
                nextJob(jobsPair, sub, ccy, rate, callbackFromJob = true)
            }

        } else {

            jobsPair.second = scope.launch(start = CoroutineStart.LAZY) {
                sub.onPrice(ccy, rate)
                nextJob(jobsPair, sub, ccy, rate, callbackFromJob = true)
            }

        }
    }

    private fun delegateJobsProcessing(sub: PriceProcessor, ccy: String, rate: Double) {
        val jobs = ccyProcessorJobsPairs.getOrPut(ccy) { ArrayList() }

        if (!jobs.any { it.first == sub }) {
            jobs += ProcessorJobsPair(sub, CurrentAndNextJobs(null, null))
        }

        jobs.filter { it.first == sub }
            .map { it.second }
            .onEach { nextJob(it, sub, ccy, rate) }
    }

    override fun subscribe(priceProcessor: PriceProcessor?) {
        priceProcessor?.let { subscribers += it }
    }

    override fun unsubscribe(priceProcessor: PriceProcessor?) {
        priceProcessor?.let { subscribers -= it }
    }
}
