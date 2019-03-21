/*
 * Copyright 2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.kafka.transaction;

import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaResourceHolder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.core.ProducerFactoryUtils;
import org.springframework.transaction.CannotCreateTransactionException;
import org.springframework.transaction.InvalidIsolationLevelException;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;
import org.springframework.transaction.support.ResourceTransactionManager;
import org.springframework.transaction.support.SmartTransactionObject;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.util.Assert;

/**
 * {@link org.springframework.transaction.PlatformTransactionManager} implementation for a
 * single Kafka {@link ProducerFactory}. Binds a Kafka producer from the specified
 * ProducerFactory to the thread, potentially allowing for one thread-bound producer per
 * ProducerFactory.
 *
 * <p>
 * This local strategy is an alternative to executing Kafka operations within, and
 * synchronized with, external transactions. This strategy is <i>not</i> able to provide
 * XA transactions, for example in order to share transactions between messaging and
 * database access.
 *
 * <p>
 * Application code is required to retrieve the transactional Kafka resources via
 * {@link ProducerFactoryUtils#getTransactionalResourceHolder(ProducerFactory)}.
 * Spring's {@link KafkaTemplate} will auto detect a thread-bound Producer and
 * automatically participate in it.
 *
 * <p>
 * <b>The use of {@link DefaultKafkaProducerFactory} as a target for this transaction
 * manager is strongly recommended.</b> Because it caches producers for reuse.
 *
 * <p>
 * Transaction synchronization is turned off by default, as this manager might be used
 * alongside a datastore-based Spring transaction manager such as the JDBC
 * org.springframework.jdbc.datasource.DataSourceTransactionManager, which has stronger
 * needs for synchronization.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Gary Russell
 */
@SuppressWarnings("serial")
public class KafkaTransactionManager<K, V> extends AbstractPlatformTransactionManager
		implements ResourceTransactionManager {

	private final ProducerFactory<K, V> producerFactory;

	/**
	 * Create a new KafkaTransactionManager, given a ConnectionFactory.
	 * Transaction synchronization is turned off by default, as this manager might be used alongside a datastore-based
	 * Spring transaction manager like DataSourceTransactionManager, which has stronger needs for synchronization. Only
	 * one manager is allowed to drive synchronization at any point of time.
	 * @param producerFactory the ProducerFactory to use
	 */
	public KafkaTransactionManager(ProducerFactory<K, V> producerFactory) {
		Assert.notNull(producerFactory, "The 'ProducerFactory' cannot be null");
		Assert.isTrue(producerFactory.transactionCapable(), "The 'ProducerFactory' must support transactions");
		setTransactionSynchronization(SYNCHRONIZATION_NEVER);
		this.producerFactory = producerFactory;
	}

	/**
	 * Get the producer factory.
	 * @return the producerFactory
	 */
	public ProducerFactory<K, V> getProducerFactory() {
		return this.producerFactory;
	}

	@Override
	public Object getResourceFactory() {
		return getProducerFactory();
	}

	@SuppressWarnings("unchecked")
	@Override
	protected Object doGetTransaction() {
		KafkaTransactionObject<K, V> txObject = new KafkaTransactionObject<K, V>();
		txObject.setResourceHolder((KafkaResourceHolder<K, V>) TransactionSynchronizationManager
				.getResource(getProducerFactory()));
		return txObject;
	}

	@Override
	protected boolean isExistingTransaction(Object transaction) {
		@SuppressWarnings("unchecked")
		KafkaTransactionObject<K, V> txObject = (KafkaTransactionObject<K, V>) transaction;
		return (txObject.getResourceHolder() != null);
	}

	@Override
	protected void doBegin(Object transaction, TransactionDefinition definition) {
		if (definition.getIsolationLevel() != TransactionDefinition.ISOLATION_DEFAULT) {
			throw new InvalidIsolationLevelException("Apache Kafka does not support an isolation level concept");
		}
		@SuppressWarnings("unchecked")
		KafkaTransactionObject<K, V> txObject = (KafkaTransactionObject<K, V>) transaction;
		KafkaResourceHolder<K, V> resourceHolder = null;
		try {
			resourceHolder = ProducerFactoryUtils.getTransactionalResourceHolder(getProducerFactory());
			if (logger.isDebugEnabled()) {
				logger.debug("Created Kafka transaction on producer [" + resourceHolder.getProducer() + "]");
			}
			txObject.setResourceHolder(resourceHolder);
			txObject.getResourceHolder().setSynchronizedWithTransaction(true);
			int timeout = determineTimeout(definition);
			if (timeout != TransactionDefinition.TIMEOUT_DEFAULT) {
				txObject.getResourceHolder().setTimeoutInSeconds(timeout);
			}
		}
		catch (Exception ex) {
			if (resourceHolder != null) {
				ProducerFactoryUtils.releaseResources(resourceHolder);
			}
			throw new CannotCreateTransactionException("Could not create Kafka transaction", ex);
		}
	}

	@Override
	protected Object doSuspend(Object transaction) {
		@SuppressWarnings("unchecked")
		KafkaTransactionObject<K, V> txObject = (KafkaTransactionObject<K, V>) transaction;
		txObject.setResourceHolder(null);
		return TransactionSynchronizationManager.unbindResource(getProducerFactory());
	}

	@Override
	protected void doResume(Object transaction, Object suspendedResources) {
		@SuppressWarnings("unchecked")
		KafkaResourceHolder<K, V> producerHolder = (KafkaResourceHolder<K, V>) suspendedResources;
		TransactionSynchronizationManager.bindResource(getProducerFactory(), producerHolder);
	}

	@Override
	protected void doCommit(DefaultTransactionStatus status) {
		@SuppressWarnings("unchecked")
		KafkaTransactionObject<K, V> txObject = (KafkaTransactionObject<K, V>) status.getTransaction();
		KafkaResourceHolder<K, V> resourceHolder = txObject.getResourceHolder();
		resourceHolder.commit();
	}

	@Override
	protected void doRollback(DefaultTransactionStatus status) {
		@SuppressWarnings("unchecked")
		KafkaTransactionObject<K, V> txObject = (KafkaTransactionObject<K, V>) status.getTransaction();
		KafkaResourceHolder<K, V> resourceHolder = txObject.getResourceHolder();
		resourceHolder.rollback();
	}

	@Override
	protected void doSetRollbackOnly(DefaultTransactionStatus status) {
		@SuppressWarnings("unchecked")
		KafkaTransactionObject<K, V> txObject = (KafkaTransactionObject<K, V>) status.getTransaction();
		txObject.getResourceHolder().setRollbackOnly();
	}

	@Override
	protected void doCleanupAfterCompletion(Object transaction) {
		@SuppressWarnings("unchecked")
		KafkaTransactionObject<K, V> txObject = (KafkaTransactionObject<K, V>) transaction;
		TransactionSynchronizationManager.unbindResource(getProducerFactory());
		txObject.getResourceHolder().close();
		txObject.getResourceHolder().clear();
	}

	/**
	 * Kafka transaction object, representing a KafkaResourceHolder. Used as transaction object by
	 * KafkaTransactionManager.
	 * @see KafkaResourceHolder
	 */
	private static class KafkaTransactionObject<K, V> implements SmartTransactionObject {

		private KafkaResourceHolder<K, V> resourceHolder;

		KafkaTransactionObject() {
			super();
		}

		public void setResourceHolder(KafkaResourceHolder<K, V> resourceHolder) {
			this.resourceHolder = resourceHolder;
		}

		public KafkaResourceHolder<K, V> getResourceHolder() {
			return this.resourceHolder;
		}

		@Override
		public boolean isRollbackOnly() {
			return this.resourceHolder.isRollbackOnly();
		}

		@Override
		public void flush() {
			// no-op
		}

	}

}
