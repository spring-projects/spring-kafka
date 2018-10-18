/*
 * Copyright 2002-2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.kafka.core;

import org.apache.kafka.clients.producer.Producer;

import org.springframework.lang.Nullable;
import org.springframework.transaction.support.ResourceHolderSynchronization;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.util.Assert;

/**
 * Helper class for managing a Spring based Kafka {@link DefaultKafkaProducerFactory}
 * in particular for obtaining transactional Kafka resources for a given ProducerFactory.
 *
 * <p>
 * Mainly for internal use within the framework.
 *
 * @author Gary Russell
 */
public final class ProducerFactoryUtils {

	private static ThreadLocal<String> groupIds = new ThreadLocal<>();

	private ProducerFactoryUtils() {
		super();
	}

	/**
	 * Obtain a Producer that is synchronized with the current transaction, if any.
	 * @param producerFactory the ProducerFactory to obtain a Channel for
	 * @param <K> the key type.
	 * @param <V> the value type.
	 * @return the resource holder.
	 */
	public static <K, V> KafkaResourceHolder<K, V> getTransactionalResourceHolder(
			final ProducerFactory<K, V> producerFactory) {

		Assert.notNull(producerFactory, "ProducerFactory must not be null");

		@SuppressWarnings("unchecked")
		KafkaResourceHolder<K, V> resourceHolder = (KafkaResourceHolder<K, V>) TransactionSynchronizationManager
				.getResource(producerFactory);
		if (resourceHolder == null) {
			Producer<K, V> producer = producerFactory.createProducer();

			try {
				producer.beginTransaction();
			}
			catch (RuntimeException e) {
				producer.close();
				throw e;
			}

			resourceHolder = new KafkaResourceHolder<K, V>(producer);
			bindResourceToTransaction(resourceHolder, producerFactory);
		}
		return resourceHolder;
	}

	public static <K, V> void releaseResources(@Nullable KafkaResourceHolder<K, V> resourceHolder) {
		if (resourceHolder != null) {
			resourceHolder.getProducer().close();
		}
	}

	/**
	 * Set the group id for the consumer bound to this thread.
	 * @param groupId the group id.
	 * @since 1.3
	 */
	public static void setConsumerGroupId(String groupId) {
		groupIds.set(groupId);
	}

	/**
	 * Get the group id for the consumer bound to this thread.
	 * @return the group id.
	 * @since 1.3
	 */
	public static String getConsumerGroupId() {
		return groupIds.get();
	}

	/**
	 * Clear the group id for the consumer bound to this thread.
	 * @since 1.3
	 */
	public static void clearConsumerGroupId() {
		groupIds.remove();
	}

	private static <K, V> void bindResourceToTransaction(KafkaResourceHolder<K, V> resourceHolder,
			ProducerFactory<K, V> producerFactory) {
		TransactionSynchronizationManager.bindResource(producerFactory, resourceHolder);
		resourceHolder.setSynchronizedWithTransaction(true);
		if (TransactionSynchronizationManager.isSynchronizationActive()) {
			TransactionSynchronizationManager
					.registerSynchronization(new KafkaResourceSynchronization<K, V>(resourceHolder, producerFactory));
		}
	}

	/**
	 * Callback for resource cleanup at the end of a non-native Kafka transaction (e.g. when participating in a
	 * JtaTransactionManager transaction).
	 * @see org.springframework.transaction.jta.JtaTransactionManager
	 */
	private static final class KafkaResourceSynchronization<K, V> extends
			ResourceHolderSynchronization<KafkaResourceHolder<K, V>, Object> {

		private final KafkaResourceHolder<K, V> resourceHolder;

		KafkaResourceSynchronization(KafkaResourceHolder<K, V> resourceHolder, Object resourceKey) {
			super(resourceHolder, resourceKey);
			this.resourceHolder = resourceHolder;
		}

		@Override
		protected boolean shouldReleaseBeforeCompletion() {
			return false;
		}

		@Override
		public void afterCompletion(int status) {
			try {
				if (status == TransactionSynchronization.STATUS_COMMITTED) {
					this.resourceHolder.commit();
				}
				else {
					this.resourceHolder.rollback();
				}
			}
			finally {
				super.afterCompletion(status);
			}
		}

		@Override
		protected void releaseResource(KafkaResourceHolder<K, V> resourceHolder, Object resourceKey) {
			ProducerFactoryUtils.releaseResources(resourceHolder);
		}

	}

}
