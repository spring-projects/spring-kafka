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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.jspecify.annotations.NonNull;
import org.reactivestreams.Processor;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Operators;
import reactor.core.publisher.SignalType;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.log.LogAccessor;
import org.springframework.kafka.entity.KafkaEntityException;
import org.springframework.kafka.entity.KafkaEntityKey;
import org.springframework.kafka.entity.KafkaEntityUtil;
import org.springframework.kafka.support.serializer.JsonKeySerializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

/**
 * Implementation class for @KafkaEntityProcessor .
 *
 * @param <T> class of the Kafka Entity, representing messages
 * @param <K> class of the Kafka Entity Key of the Kafka Entity
 *
 * @author Popovics Boglarka
 */
public final class SimpleKafkaEntityProcessor<T, K> extends Flux<RecordMetadata>
		implements KafkaProcessor<T>, Processor<T, RecordMetadata>, DisposableBean, InitializingBean {

	private final LogAccessor logger = new LogAccessor(LogFactory.getLog(getClass()));

	/** The terminated indicator for the subscribers array. */
	@SuppressWarnings("rawtypes")
	static final ProcessorWrapper[] TERMINATED = new ProcessorWrapper[0];

	/** An empty subscribers array to avoid allocating it all the time. */
	@SuppressWarnings("rawtypes")
	static final ProcessorWrapper[] EMPTY = new ProcessorWrapper[0];

	/** The array of currently subscribed subscribers. */
	final AtomicReference<ProcessorWrapper<T, K>[]> subscribers;

	private String clientid;

	private KafkaProducer<K, T> kafkaProducer;

	private Class<T> clazz;

	private String topic;

	/** The error, write before terminating and read after checking subscribers. */
	Throwable error;

	private Field keyField;

	private boolean transactional;

	private List<String> bootstrapServers;

	/**
	 * Constructs a SimpleKafkaEntityProcessor.
	 *
	 * @param <T>                  the value type
	 * @param <K>                  the key type
	 * @param bootstrapServers     List of bootstrapServers
	 * @param kafkaEntityProcessor annotation
	 * @param entity               class of the Kafka Entity, representing messages
	 * @param beanName             name of the Kafka Entity Bean
	 *
	 * @return the new SimpleKafkaEntityProcessor
	 * @throws KafkaEntityException problem at creation
	 */
	// @CheckReturnValue
	@NonNull
	public static <T, K> SimpleKafkaEntityProcessor<T, K> create(List<String> bootstrapServers,
			KafkaEntityProcessor kafkaEntityProcessor, Class<T> entity, String beanName) throws KafkaEntityException {
		return new SimpleKafkaEntityProcessor<>(bootstrapServers, kafkaEntityProcessor, entity, beanName);
	}

	SimpleKafkaEntityProcessor(List<String> bootstrapServers, KafkaEntityProcessor kafkaEntityProcessor,
			Class<T> entity, String beanName) {

		this.bootstrapServers = bootstrapServers;
		this.subscribers = new AtomicReference<>(EMPTY);

		this.clazz = entity;
		this.clientid = beanName;
		this.topic = KafkaEntityUtil.getTopicName(this.clazz);

		this.transactional = kafkaEntityProcessor.transactional();

		// presents of @KafkaEntityKey is checked in KafkaEntityConfig
		for (Field field : this.clazz.getDeclaredFields()) {
			if (this.logger.isDebugEnabled()) {
				this.logger.debug("    field  -> " + field.getName());
			}
			if (field.isAnnotationPresent(KafkaEntityKey.class)) {
				field.setAccessible(true);
				this.keyField = field;
				break;
			}
		}

		Map<String, Object> configProps = new HashMap<>();
		configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, JsonKeySerializer.class);
		configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
		configProps.put(ProducerConfig.CLIENT_ID_CONFIG, this.clientid);

		if (this.transactional) {
			configProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, beanName + "-transactional-id");
			configProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
		}

		JsonSerializer<T> valueSerializer = new JsonSerializer<>();
		JsonKeySerializer<K> keySerializer = new JsonKeySerializer<>();

		this.kafkaProducer = new KafkaProducer<K, T>(configProps, keySerializer, valueSerializer);
		if (this.transactional) {
			this.logger.info("initTransactions-begin " + beanName);
			this.kafkaProducer.initTransactions();
			this.logger.info("initTransactions-end " + beanName);
		}
	}

	private K extractKey(T event) throws IllegalArgumentException, IllegalAccessException {
		return (K) this.keyField.get(event);
	}

	@Override
	public void onSubscribe(Subscription d) {

		if (this.subscribers.get() == TERMINATED) {
			d.cancel();
		}

		if (this.transactional) {
			this.kafkaProducer.beginTransaction();
		}
		d.request(Long.MAX_VALUE);
	}

	@Override
	public void onNext(T t) {
		this.logger.info("onNext: " + t);
		CompletableFuture<RecordMetadata> completableFuture = new CompletableFuture<>();
		K key;
		try {
			key = extractKey(t);

			ProducerRecord<K, T> rec = new ProducerRecord<K, T>(this.topic, key, t);
			Future<RecordMetadata> send = this.kafkaProducer.send(rec);
			completableFuture.complete(send.get());
		}
		catch (InterruptedException | ExecutionException e) {
			completableFuture.completeExceptionally(e);
		}
		catch (IllegalAccessException e) {
			completableFuture.completeExceptionally(e);
		}

		try {
			RecordMetadata r = completableFuture.get();
			for (ProcessorWrapper<T, K> pd : this.subscribers.get()) {
				pd.onNext(r);
			}
		}
		catch (InterruptedException | ExecutionException e) {
			onError(e);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public void onError(Throwable t) {
		if (this.subscribers.get() == TERMINATED) {
			onError(t);
			return;
		}
		this.error = t;

		for (ProcessorWrapper<T, K> pd : this.subscribers.getAndSet(TERMINATED)) {
			pd.onError(t);
		}
		if (this.transactional) {
			this.kafkaProducer.abortTransaction();
		}
		return;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void onComplete() {
		if (this.subscribers.get() == TERMINATED) {
			return;
		}
		for (ProcessorWrapper<T, K> pd : this.subscribers.getAndSet(TERMINATED)) {
			pd.onComplete();
		}
		if (this.transactional) {
			this.kafkaProducer.commitTransaction();
		}
	}

	@Override
	public void afterPropertiesSet() throws Exception {

	}

	@Override
	public void destroy() throws Exception {
		this.kafkaProducer.close();
	}

	@Override
	public void subscribe(CoreSubscriber<? super RecordMetadata> actual) {
		ProcessorWrapper<T, K> ps = new ProcessorWrapper<>(actual, this);
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
	boolean add(ProcessorWrapper<T, K> ps) {
		for (;;) {
			ProcessorWrapper<T, K>[] a = this.subscribers.get();
			if (a == TERMINATED) {
				return false;
			}

			int n = a.length;
			@SuppressWarnings("unchecked")
			ProcessorWrapper<T, K>[] b = new ProcessorWrapper[n + 1];
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
	void remove(ProcessorWrapper<T, K> ps) {
		for (;;) {
			ProcessorWrapper<T, K>[] a = this.subscribers.get();
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

			ProcessorWrapper<T, K>[] b;

			if (n == 1) {
				b = EMPTY;
			}
			else {
				b = new ProcessorWrapper[n - 1];
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
	static final class ProcessorWrapper<T, K> extends AtomicBoolean
			implements CoreSubscriber<RecordMetadata>, Subscription, Disposable {

		private static final long serialVersionUID = 1L;

		private final LogAccessor logger = new LogAccessor(LogFactory.getLog(getClass()));

		/** The actual subscriber. */
		final CoreSubscriber<? super RecordMetadata> downstream;

		/** The Processor state. */
		final SimpleKafkaEntityProcessor<T, K> parent;

		/**
		 * Constructs a ProcessorWrapper, wraps the actual subscriber and the state.
		 *
		 * @param actual the actual subscriber
		 * @param parent the parent PublishProcessor
		 */
		ProcessorWrapper(CoreSubscriber<? super RecordMetadata> actual, SimpleKafkaEntityProcessor<T, K> parent) {
			this.downstream = actual;
			this.parent = parent;
		}

		@Override
		public void dispose() {
			if (compareAndSet(false, true)) {
				this.parent.remove(this);
			}
			cancel();
		}

		volatile Subscription subscription;

		static AtomicReferenceFieldUpdater<ProcessorWrapper, Subscription> S =
				AtomicReferenceFieldUpdater.newUpdater(ProcessorWrapper.class, Subscription.class, "subscription");

		/**
		 * Return current {@link Subscription} .
		 *
		 * @return current {@link Subscription}
		 */
		protected Subscription upstream() {
			return this.subscription;
		}

		@Override
		public boolean isDisposed() {
			return this.subscription == Operators.cancelledSubscription();
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.setOnce(S, this, s)) {
				try {
					this.downstream.onSubscribe(s);
					this.subscription.request(Long.MAX_VALUE);
				}
				catch (Throwable throwable) {
					onError(Operators.onOperatorError(s, throwable, currentContext()));
				}
			}
		}

		@Override
		public void onNext(RecordMetadata value) {
			Objects.requireNonNull(value, "onNext");
			try {
				if (!get()) {
					this.downstream.onNext(value);
				}
			}
			catch (Throwable throwable) {
				onError(Operators.onOperatorError(this.subscription, throwable, value, currentContext()));
			}
		}

		@Override
		public void onError(Throwable t) {
			Objects.requireNonNull(t, "onError");

			if (S.getAndSet(this, Operators.cancelledSubscription()) == Operators
					.cancelledSubscription()) {
				//already cancelled concurrently
				Operators.onErrorDropped(t, currentContext());
				return;
			}

			try {
				if (!get()) {
					this.downstream.onError(t);
				}
				throw Exceptions.errorCallbackNotImplemented(t);
			}
			catch (Throwable e) {
				e = Exceptions.addSuppressed(e, t);
				Operators.onErrorDropped(e, currentContext());
			}
			finally {
				safeHookFinally(SignalType.ON_ERROR);
			}
		}

		@Override
		public void onComplete() {
			if (S.getAndSet(this, Operators.cancelledSubscription()) != Operators
					.cancelledSubscription()) {
				//we're sure it has not been concurrently cancelled
				try {
					if (!get()) {
						this.downstream.onComplete();
					}
				}
				catch (Throwable throwable) {
					//onError itself will short-circuit due to the CancelledSubscription being set above
					onError(Operators.onOperatorError(throwable, currentContext()));
				}
				finally {
					safeHookFinally(SignalType.ON_COMPLETE);
				}
			}
		}

		@Override
		public void request(long n) {
			if (Operators.validate(n)) {
				Subscription s = this.subscription;
				if (s != null) {
					s.request(n);
				}
			}
		}

		/**
		 * {@link #request(long) Request} an unbounded amount.
		 */
		public void requestUnbounded() {
			request(Long.MAX_VALUE);
		}

		@Override
		public void cancel() {
			if (Operators.terminate(S, this)) {
				try {
					// do nothing
				}
				catch (Throwable throwable) {
					onError(Operators.onOperatorError(this.subscription, throwable, currentContext()));
				}
				finally {
					safeHookFinally(SignalType.CANCEL);
				}
			}
		}

		void safeHookFinally(SignalType type) {
			try {
				// do nothing
			}
			catch (Throwable finallyFailure) {
				Operators.onErrorDropped(finallyFailure, currentContext());
			}
		}

		@Override
		public String toString() {
			return getClass().getSimpleName();
		}
	}
}
