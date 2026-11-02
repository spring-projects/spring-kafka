/*
 * Copyright 2023-present the original author or authors.
 */

package org.springframework.kafka.core;

import org.apache.kafka.clients.producer.Producer;
import org.junit.jupiter.api.Test;

import org.springframework.core.Ordered;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * @author Gary Russell
 * @since 2.9.7
 *
 */
public class TransactionSynchronizationTests {

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	void commitAfterAnotherSyncFails() {
		Producer producer = mock(Producer.class);
		ProducerFactory pf = mock(ProducerFactory.class);
		given(pf.createProducer(any())).willReturn(producer);
		assertThatExceptionOfType(RuntimeException.class)
				.isThrownBy(() ->
					new TransactionTemplate(new TM()).executeWithoutResult(status -> {
						KafkaResourceHolder holder = ProducerFactoryUtils.getTransactionalResourceHolder(pf);
						TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {

							@Override
							public void afterCommit() {
								if (true) {
									throw new RuntimeException("Test");
								}
							}

							@Override
							public int getOrder() {
								return Ordered.HIGHEST_PRECEDENCE;
							}

						});
					}))
				.withMessage("Test");
		verify(producer).beginTransaction();
		verify(producer).commitTransaction();
		verify(producer).close(any());
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	void onlyOnceCommit() {
		Producer producer = mock(Producer.class);
		ProducerFactory pf = mock(ProducerFactory.class);
		given(pf.createProducer(any())).willReturn(producer);
		new TransactionTemplate(new TM()).executeWithoutResult(status -> {
			KafkaResourceHolder holder = ProducerFactoryUtils.getTransactionalResourceHolder(pf);
		});
		verify(producer).beginTransaction();
		verify(producer).commitTransaction();
		verify(producer).close(any());
	}

	@SuppressWarnings("serial")
	static class TM extends AbstractPlatformTransactionManager {

		@Override
		protected Object doGetTransaction() throws TransactionException {
			return new Object();
		}

		@Override
		protected void doBegin(Object transaction, TransactionDefinition definition) throws TransactionException {
		}

		@Override
		protected void doCommit(DefaultTransactionStatus status) throws TransactionException {
		}

		@Override
		protected void doRollback(DefaultTransactionStatus status) throws TransactionException {
		}

	}

}

