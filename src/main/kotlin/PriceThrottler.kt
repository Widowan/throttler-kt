import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import kotlin.coroutines.CoroutineContext

class PriceThrottler() : PriceProcessor {
    private val subscribers: MutableList<PriceProcessor>;
    private var events: Flow<Event>? = null;
    private val scope: CoroutineScope;

    data class Event(val ccyPair: String, val rate: Double);

    init {
        subscribers = ArrayList();
        scope = CoroutineScope(SupervisorJob());
    }

    override fun onPrice(ccyPair: String?, rate: Double) {
        ccyPair?.let {
            subscribers.forEach {
                scope.launch { it.onPrice(ccyPair, rate) };
            }
        }
    }

    override fun subscribe(priceProcessor: PriceProcessor?) {
        priceProcessor?.let { subscribers += it };
    }

    override fun unsubscribe(priceProcessor: PriceProcessor?) {
        priceProcessor?.let { subscribers -= it };
    }
}
