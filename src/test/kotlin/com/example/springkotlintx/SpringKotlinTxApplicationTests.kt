package com.example.springkotlintx

import com.zaxxer.hikari.HikariDataSource
import kotlinx.coroutines.Dispatchers.Default
import kotlinx.coroutines.Dispatchers.IO
import kotlinx.coroutines.ThreadContextElement
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.springframework.jdbc.datasource.DataSourceTransactionManager
import org.springframework.transaction.support.TransactionSynchronization
import org.springframework.transaction.support.TransactionSynchronizationManager
import org.springframework.transaction.support.TransactionTemplate
import kotlin.coroutines.AbstractCoroutineContextElement
import kotlin.coroutines.CoroutineContext

typealias TSM = TransactionSynchronizationManager

class SpringKotlinTxApplicationTests {

    @Test
    fun test() {

        HikariDataSource().use { ds ->
            ds.jdbcUrl = "jdbc:h2:mem:"

            val txManager = DataSourceTransactionManager(ds)
            val txTpl = TransactionTemplate(txManager)

            assertThat(inTx()).isFalse
            txTpl.execute {
                assertThat(inTx()).isTrue

                runBlocking {
                    assertThat(inTx()).isTrue
                    launch(Default) {
                        // this is expected... by default the spring transaction is not carried over to other threads
                        assertThat(inTx()).isFalse
                        launch(IO) {
                            assertThat(inTx()).isFalse
                            withContext(Default) {
                                assertThat(inTx()).isFalse
                            }
                        }
                    }
                }
            }

            assertThat(inTx()).isFalse
            txTpl.execute {
                assertThat(inTx()).isTrue

                val tx = SpringTxState()

                runBlocking {
                    assertThat(inTx()).isTrue
                    assertThat(SpringTxState()).isEqualTo(tx)
                    launch(Default + SpringTxContext()) { // adding our own context to carry the spring tx with us
                        assertThat(inTx()).isTrue
                        assertThat(SpringTxState()).isEqualTo(tx)
                        launch(IO) {
                            assertThat(inTx()).isTrue
                            assertThat(SpringTxState()).isEqualTo(tx)
                            withContext(Default) {
                                assertThat(inTx()).isTrue
                                assertThat(SpringTxState()).isEqualTo(tx)
                            }
                        }
                    }
                }
            }
        }
    }

    private fun inTx() = TSM.isActualTransactionActive()

}

class SpringTxContext(
    private val state: SpringTxState = SpringTxState()
) : ThreadContextElement<SpringTxState>, AbstractCoroutineContextElement(Key) {
    companion object Key : CoroutineContext.Key<SpringTxContext>

    override fun restoreThreadContext(context: CoroutineContext, oldState: SpringTxState) {
        require(state == oldState.applyOnCurrentThread())
    }

    override fun updateThreadContext(context: CoroutineContext): SpringTxState =
        state.applyOnCurrentThread()
}

data class SpringTxState(
    val isTransactionActive: Boolean = TSM.isActualTransactionActive(),
    val currentTransactionReadOnly: Boolean = TSM.isCurrentTransactionReadOnly(),
    val currentTransactionIsolationLevel: Int? = TSM.getCurrentTransactionIsolationLevel(),
    val currentTransactionName: String? = TSM.getCurrentTransactionName(),
    val resourceMap: Map<Any, Any> = TSM.getResourceMap().toMap(), // need to copy the Map!
    val synchronizationActive: Boolean = TSM.isSynchronizationActive(),
    val synchronizations: List<TransactionSynchronization>? = if (synchronizationActive) TSM.getSynchronizations() else null
) {
    init {
        if (synchronizationActive) require(synchronizations != null)
        else require(synchronizations == null)
    }

    fun isEmpty(): Boolean = !isTransactionActive
            && !currentTransactionReadOnly
            && currentTransactionIsolationLevel == null
            && currentTransactionName == null
            && resourceMap.isEmpty()
            && !synchronizationActive
            && synchronizations == null

    fun applyOnCurrentThread(): SpringTxState {
        val oldState = SpringTxState()
        TSM.clear()
        // clear() clears everything except the resources
        oldState.resourceMap.keys.forEach { TSM.unbindResource(it) }

        TSM.setActualTransactionActive(isTransactionActive)
        TSM.setCurrentTransactionReadOnly(currentTransactionReadOnly)
        TSM.setCurrentTransactionIsolationLevel(currentTransactionIsolationLevel)
        TSM.setCurrentTransactionName(currentTransactionName)
        resourceMap.forEach { (k, v) -> TSM.bindResource(k, v) }
        if (synchronizationActive) {
            require(synchronizations != null)
            TSM.initSynchronization()
            synchronizations.forEach { TSM.registerSynchronization(it) }
        } else {
            require(synchronizations == null)
        }

        require(this == SpringTxState())

        return oldState
    }
}
