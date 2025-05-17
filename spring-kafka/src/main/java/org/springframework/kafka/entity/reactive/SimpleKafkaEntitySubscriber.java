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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.jspecify.annotations.NonNull;
import org.reactivestreams.Subscription;
import reactor.core.publisher.BaseSubscriber;
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
 * Implementation class for @KafkaEntitySubscriber.
 *
 * @param <T> class of the entity, representing messages
 * @param <K> the key from the entity
 *
 * @author Popovics Boglarka
 */
public final class SimpleKafkaEntitySubscriber<T, K> extends BaseSubscriber<T> implements DisposableBean, InitializingBean {

	private final LogAccessor logger = new LogAccessor(LogFactory.getLog(getClass()));

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
	 * Constructs a SimpleKafkaSubscriber.
	 *
	 * @param <T>                   the value type
	 * @param <K>                   the key type
	 * @param bootstrapServers      List of bootstrapServers
	 * @param kafkaEntitySubscriber annotation
	 * @param entity                class of the Kafka Entity, representing messages
	 * @param beanName              name of the Kafka Entity Bean
	 *
	 * @return the new SimpleKafkaSubscriber
	 * @throws KafkaEntityException problem at creation
	 */
	@NonNull
	public static <T, K> SimpleKafkaEntitySubscriber<T, K> create(List<String> bootstrapServers, KafkaEntitySubscriber kafkaEntitySubscriber, Class<T> entity,
			String beanName) throws KafkaEntityException {
		return new SimpleKafkaEntitySubscriber<>(bootstrapServers, kafkaEntitySubscriber, entity, beanName);
	}

	SimpleKafkaEntitySubscriber(List<String> bootstrapServers, KafkaEntitySubscriber kafkaEntitySubscriber, Class<T> entity, String beanName) throws KafkaEntityException {
		this.bootstrapServers = bootstrapServers;
		this.clazz = entity;
		this.clientid = beanName;
		this.topic = KafkaEntityUtil.getTopicName(this.clazz);
		this.transactional = kafkaEntitySubscriber.transactional();

		// presents of @KafkaEntityKey is checked in KafkaEntityConfig
		for (Field field : this.clazz.getDeclaredFields()) {
			if (this.logger.isDebugEnabled())  {
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
	public void hookOnSubscribe(Subscription d) {
		if (this.transactional) {
			this.kafkaProducer.beginTransaction();
		}
		super.hookOnSubscribe(d);
	}

	@Override
	public void hookOnNext(T t) {
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
			completableFuture.get();
		}
		catch (InterruptedException | ExecutionException e) {
			onError(e);
		}

		super.hookOnNext(t);
	}

	@Override
	public void hookOnError(Throwable t) {
		this.logger.error(t, "onError");
		if (this.transactional) {
			this.kafkaProducer.abortTransaction();
		}
		super.hookOnError(t);
	}

	@Override
	public void hookOnComplete() {
		if (this.transactional) {
			this.kafkaProducer.commitTransaction();
		}
		super.hookOnComplete();
	}

	@Override
	public void afterPropertiesSet() throws Exception {

	}

	@Override
	public void destroy() throws Exception {
		this.kafkaProducer.close();
	}

	@Override
	protected void hookFinally(SignalType type) {
		super.hookFinally(type);
	}

	@Override
	protected void hookOnCancel() {
		super.hookOnCancel();
	}

}
