/*
 * Copyright 2016-2025 the original author or authors.
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

package org.springframework.kafka.entity.reactive;

import java.lang.reflect.Field;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DeleteConsumerGroupsResult;
import org.apache.kafka.common.KafkaFuture;
import org.jspecify.annotations.NonNull;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.log.LogAccessor;
import org.springframework.kafka.entity.KafkaEntityException;
import org.springframework.kafka.entity.KafkaEntityKey;

/**
 * Implementation class for @KafkaEntityPublisher .
 *
 * @param <T> class of the entity, representing messages
 * @param <K> the key from the entity
 *
 * @author Popovics Boglarka
 */
public class SimpleKafkaEntityPublisher<T, K> extends Flux<T> implements DisposableBean, InitializingBean {

	private final LogAccessor logger = new LogAccessor(LogFactory.getLog(getClass()));

	private List<String> bootstrapServers;

	private String groupid;

	private Class<T> clazz;

	private Class<K> clazzKey;

	private final KafkaEntityPollingRunnable<T, K> pollingRunnable;

	private String beanName;

	SimpleKafkaEntityPublisher(List<String> bootstrapServers, KafkaEntityPublisher kafkaEntityPublisher, Class<T> entity, String beanName)
			throws KafkaEntityException {
		this.bootstrapServers = bootstrapServers;
		this.clazz = entity;
		this.groupid = /* getTopicName() + "-Subscriber-" + */ beanName;

		// presents of @KafkaEntityKey is checked in KafkaEntityConfig
		for (Field field : this.clazz.getDeclaredFields()) {
			if (this.logger.isDebugEnabled()) {
				this.logger.debug("    field  -> " + field.getName());
			}
			if (field.isAnnotationPresent(KafkaEntityKey.class)) {
				field.setAccessible(true);
				this.clazzKey = (Class<K>) field.getType();
				break;
			}
		}

		this.subscribers = new AtomicReference<>(EMPTY);

		this.beanName = beanName;

		this.pollingRunnable = new KafkaEntityPollingRunnable<T, K>(this.groupid, this.clazz, this.clazzKey, this.subscribers,
				bootstrapServers, beanName);

		start();
	}

	private void start() throws KafkaEntityException {

		if (this.pollingRunnable.getStarted().get()) {
			this.logger.warn("already started...");
			return;
		}

		if (this.logger.isDebugEnabled()) {
			this.logger.debug("starting " + this.beanName + "...");
		}

		Thread pollingThread = new Thread(this.pollingRunnable);
		pollingThread.setName(this.beanName + "Thread");
		pollingThread.start();

		this.logger.info("waiting the consumer to start in " + this.beanName + "Thread...");
		LocalDateTime then = LocalDateTime.now();
		while (!this.pollingRunnable.getStarted().get()) {
			if (ChronoUnit.SECONDS.between(then, LocalDateTime.now()) >= 300) {
				throw new KafkaEntityException(this.beanName, "KafkaConsumer could not start in 300 sec.");
			}
		}
	}

	@Override
	public void afterPropertiesSet() throws Exception {

	}

	@Override
	public void destroy() throws Exception {

		this.logger.warn("deleting a Consumer Group for " + this.beanName);

		this.pollingRunnable.getStopped().set(true);

		this.logger.info("waiting polling to stop");
		LocalDateTime then = LocalDateTime.now();
		while (this.pollingRunnable.getStarted().get()) {
			if (ChronoUnit.SECONDS.between(then, LocalDateTime.now()) >= 20) {
				break;
			}
		}

		final Properties properties = new Properties();
		properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServers);
		properties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "1000");
		properties.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "5000");

		try (AdminClient adminClient = AdminClient.create(properties);) {
			String consumerGroupToBeDeleted = this.groupid;
			DeleteConsumerGroupsResult deleteConsumerGroupsResult = adminClient
					.deleteConsumerGroups(Arrays.asList(consumerGroupToBeDeleted));

			KafkaFuture<Void> resultFuture = deleteConsumerGroupsResult.all();
			resultFuture.get();
		}
		catch (Exception e) {
			// we cannot do anything at this point
			this.logger.error(e.getMessage());
		}

	}

	/** The terminated indicator for the subscribers array. */
	@SuppressWarnings("rawtypes")
	static final PublisherWrapper[] TERMINATED = new PublisherWrapper[0];

	/** An empty subscribers array to avoid allocating it all the time. */
	@SuppressWarnings("rawtypes")
	static final PublisherWrapper[] EMPTY = new PublisherWrapper[0];

	/** The array of currently subscribed subscribers. */
	final AtomicReference<PublisherWrapper<T, K>[]> subscribers;

	/** The error, write before terminating and read after checking subscribers. */
	Throwable error;

	/**
	 * Constructs a SimpleKafkaPublisherHandler.
	 *
	 * @param <T>                  the value type
	 * @param <K>                  the key type
	 * @param bootstrapServers     List of bootstrapServers
	 * @param kafkaEntityPublisher annotation
	 * @param entity               class of the Kafka Entity, representing messages
	 * @param beanName             name of the Kafka Entity Bean
	 *
	 * @return the new SimpleKafkaPublisherHandler
	 * @throws KafkaEntityException problem at creation
	 */
	// @CheckReturnValue
	@NonNull
	public static <T, K> SimpleKafkaEntityPublisher<T, K> create(List<String> bootstrapServers, KafkaEntityPublisher kafkaEntityPublisher,
			Class<T> entity, String beanName) throws KafkaEntityException {
		return new SimpleKafkaEntityPublisher<T, K>(bootstrapServers, kafkaEntityPublisher, entity, beanName);
	}

	@Override
	public void subscribe(CoreSubscriber<? super T> actual) {
		PublisherWrapper<T, K> ps = new PublisherWrapper<>(actual, this);
		actual.onSubscribe(ps);
		if (add(ps)) {
			// if cancellation happened while a successful add, the remove() didn't work
			// so we need to do it again
			if (ps.isDisposed()) {
				remove(ps);
			}
		}
		else {
			Throwable ex = this.error;
			if (ex != null) {
				actual.onError(ex);
			}
			else {
				actual.onComplete();
			}
		}

	}

	/**
	 * Tries to add the given subscriber to the subscribers array atomically or
	 * returns false if the Processor has terminated.
	 *
	 * @param ps the subscriber to add
	 *
	 * @return true if successful, false if the Processor has terminated
	 */
	boolean add(PublisherWrapper<T, K> ps) {
		for (;;) {
			PublisherWrapper<T, K>[] a = this.subscribers.get();
			if (a == TERMINATED) {
				return false;
			}

			int n = a.length;
			@SuppressWarnings("unchecked")
			PublisherWrapper<T, K>[] b = new PublisherWrapper[n + 1];
			System.arraycopy(a, 0, b, 0, n);
			b[n] = ps;

			if (this.subscribers.compareAndSet(a, b)) {
				return true;
			}
		}
	}

	/**
	 * Atomically removes the given subscriber if it is subscribed to the Processor.
	 *
	 * @param ps the Processor to remove
	 */
	@SuppressWarnings("unchecked")
	void remove(PublisherWrapper<T, K> ps) {
		for (;;) {
			PublisherWrapper<T, K>[] a = this.subscribers.get();
			if (a == TERMINATED || a == EMPTY) {
				return;
			}

			int n = a.length;
			int j = -1;
			for (int i = 0; i < n; i++) {
				if (a[i] == ps) {
					j = i;
					break;
				}
			}

			if (j < 0) {
				return;
			}

			PublisherWrapper<T, K>[] b;

			if (n == 1) {
				b = EMPTY;
			}
			else {
				b = new PublisherWrapper[n - 1];
				System.arraycopy(a, 0, b, 0, j);
				System.arraycopy(a, j + 1, b, j, n - j - 1);
			}
			if (this.subscribers.compareAndSet(a, b)) {
				return;
			}
		}
	}

	/**
	 * Wraps the actual subscriber, tracks its requests and makes cancellation to
	 * remove itself from the current subscribers array.
	 *
	 * @param <T> the value type
	 * @param <K> the key type
	 */
	static final class PublisherWrapper<T, K> extends AtomicBoolean implements CoreSubscriber<T>, Subscription, Disposable {

		/** The actual subscriber. */
		final CoreSubscriber<? super T> downstream;

		/** The Processor state. */
		final SimpleKafkaEntityPublisher<T, K> parent;

		/**
		 * Constructs a PublishSubscriber, wraps the actual subscriber and the state.
		 *
		 * @param actual the actual subscriber
		 * @param parent the parent PublishProcessor
		 */
		PublisherWrapper(CoreSubscriber<? super T> actual, SimpleKafkaEntityPublisher<T, K> parent) {
			this.downstream = actual;
			this.parent = parent;
		}

		@Override
		public void dispose() {
			if (compareAndSet(false, true)) {
				this.parent.remove(this);
			}
		}

		@Override
		public void onSubscribe(Subscription s) {
			this.downstream.onSubscribe(s);
		}

		@Override
		public void onNext(T t) {
			if (!get()) {
				this.downstream.onNext(t);
			}
		}

		@Override
		public void onError(Throwable t) {
			this.downstream.onError(t);
		}

		@Override
		public void onComplete() {
			if (!get()) {
				this.downstream.onComplete();
			}
		}

		@Override
		public void request(long n) {
			compareAndSet(true, false);
		}

		@Override
		public void cancel() {
			dispose();
		}

	}

}
