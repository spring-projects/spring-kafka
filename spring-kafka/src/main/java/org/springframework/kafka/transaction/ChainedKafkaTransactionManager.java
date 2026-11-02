/*
 * Copyright 2018-present the original author or authors.
 */

package org.springframework.kafka.transaction;

import org.springframework.kafka.core.ProducerFactory;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.util.Assert;

/**
 * A {@link org.springframework.data.transaction.ChainedTransactionManager} that has
 * exactly one {@link KafkaAwareTransactionManager} in the chain.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Gary Russell
 * @since 2.1.3
 * @deprecated Refer to the
 * {@link org.springframework.data.transaction.ChainedTransactionManager} javadocs.
 *
 */
@Deprecated
public class ChainedKafkaTransactionManager<K, V> extends org.springframework.data.transaction.ChainedTransactionManager
		implements KafkaAwareTransactionManager<K, V> {

	private final KafkaAwareTransactionManager<K, V> kafkaTransactionManager;

	/**
	 * Construct an instance with the provided {@link PlatformTransactionManager}s.
	 * @param transactionManagers the transaction managers.
	 */
	@SuppressWarnings("unchecked")
	public ChainedKafkaTransactionManager(PlatformTransactionManager... transactionManagers) {
		super(transactionManagers);
		KafkaAwareTransactionManager<K, V> uniqueKafkaTransactionManager = null;
		for (PlatformTransactionManager tm : transactionManagers) {
			if (tm instanceof KafkaAwareTransactionManager) {
				Assert.isNull(uniqueKafkaTransactionManager, "Only one KafkaAwareTransactionManager is allowed");
				uniqueKafkaTransactionManager = (KafkaAwareTransactionManager<K, V>) tm;
			}
		}
		Assert.notNull(uniqueKafkaTransactionManager, "Exactly one KafkaAwareTransactionManager is required");
		this.kafkaTransactionManager = uniqueKafkaTransactionManager;
	}

	@Override
	public ProducerFactory<K, V> getProducerFactory() {
		return this.kafkaTransactionManager.getProducerFactory();
	}

}
