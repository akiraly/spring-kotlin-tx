package com.example.springkotlintx.reactor

import com.example.springkotlintx.SpringTxState
import com.zaxxer.hikari.HikariDataSource
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.springframework.jdbc.datasource.DataSourceTransactionManager
import org.springframework.transaction.support.TransactionSynchronizationManager
import org.springframework.transaction.support.TransactionTemplate
import reactor.core.publisher.Mono
import reactor.core.scheduler.Schedulers
import reactor.core.scheduler.Schedulers.boundedElastic
import reactor.core.scheduler.Schedulers.parallel
import java.time.Duration

typealias TSM = TransactionSynchronizationManager

class SpringReactorTxApplicationTests {

    @Test
    fun test() {
        Schedulers.onScheduleHook("spring-tx") { r ->
            val state = SpringTxState()

            if (state.isEmpty()) return@onScheduleHook r

            Runnable {
                val oldState = state.applyOnCurrentThread()
                try {
                    r.run()
                } finally {
                    assertThat(oldState.applyOnCurrentThread()).isEqualTo(state)
                }
            }
        }

        HikariDataSource().use { ds ->
            ds.jdbcUrl = "jdbc:h2:mem:"

            val txManager = DataSourceTransactionManager(ds)
            val txTpl = TransactionTemplate(txManager)

            assertThat(inTx()).isFalse
            txTpl.execute {
                val tx = SpringTxState()

                assertThat(inTx()).isTrue

                Mono.defer {
                    assertThat(inTx()).isTrue
                    assertThat(SpringTxState()).isEqualTo(tx)
                    Mono.just(5)
                }
                    .publishOn(boundedElastic())
                    .doOnNext {
                        assertThat(inTx()).isTrue
                        assertThat(SpringTxState()).isEqualTo(tx)
                    }
                    .publishOn(parallel())
                    .doOnNext {
                        assertThat(inTx()).isTrue
                        assertThat(SpringTxState()).isEqualTo(tx)
                    }
                    .publishOn(boundedElastic())
                    .doOnNext {
                        assertThat(inTx()).isTrue
                        assertThat(SpringTxState()).isEqualTo(tx)
                    }.block(Duration.ofMillis(100))
            }
        }
    }

    private fun inTx() = TSM.isActualTransactionActive()

}
