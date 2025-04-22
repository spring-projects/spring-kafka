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

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import org.springframework.core.log.LogAccessor;
import org.springframework.kafka.entity.KafkaEntityUtil;
import org.springframework.kafka.entity.reactive.SimpleKafkaEntityPublisher.PublisherWrapper;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonKeyDeserializer;

/**
 * This runnable is constantly polling a kafka-consumer for 10 sec after started
 * until it is stopped if there is minimum 1 subscriber.
 *
 * @param <T> class of the entity, representing messages
 * @param <K> the key from the entity
 *
 * @author Popovics Boglarka
 */
public class KafkaEntityPollingRunnable<T, K> implements Runnable {

	private final LogAccessor logger = new LogAccessor(LogFactory.getLog(getClass()));

	private final String groupid;

	private final Class<T> clazz;

	private final Class<K> clazzKey;

	private final AtomicBoolean stopped = new AtomicBoolean(false);

	private final AtomicBoolean started = new AtomicBoolean(false);

	/** The array of currently subscribed subscribers. */
	private final AtomicReference<PublisherWrapper<T, K>[]> subscribers;

	private final List<String> bootstrapServers;

	private final String beanName;

	KafkaEntityPollingRunnable(String groupid, Class<T> clazz, Class<K> clazzKey,
			AtomicReference<PublisherWrapper<T, K>[]> subscribers, List<String> bootstrapServers,
			String beanName) {
		super();
		this.groupid = groupid;
		this.clazz = clazz;
		this.clazzKey = clazzKey;
		this.subscribers = subscribers;
		this.bootstrapServers = bootstrapServers;
		this.beanName = beanName;
	}

	@Override
	public void run() {

		Map<String, Object> properties = new HashMap<>();
		properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServers);
		properties.put(ConsumerConfig.GROUP_ID_CONFIG, this.groupid);

		properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
		JsonDeserializer<T> valueDeserializer = new JsonDeserializer<>(getClazz());
		JsonKeyDeserializer<K> keyDeserializer = new JsonKeyDeserializer<>(getClazzKey());

		valueDeserializer.addTrustedPackages(getClazz().getPackageName());
		keyDeserializer.addTrustedPackages(getClazzKey().getPackageName());

		try (KafkaConsumer<K, T> kafkaConsumer = new KafkaConsumer<K, T>(properties, keyDeserializer,
				valueDeserializer);) {

			kafkaConsumer.subscribe(List.of(KafkaEntityUtil.getTopicName(getClazz())));

			kafkaConsumer.poll(Duration.ofSeconds(10L));

			kafkaConsumer.seekToEnd(Collections.emptyList());
			kafkaConsumer.commitSync();

			// wait until kafkaConsumer is ready and offset setted

			LocalDateTime then = LocalDateTime.now();
			while (kafkaConsumer.committed(kafkaConsumer.assignment()).isEmpty()) {
				if (ChronoUnit.SECONDS.between(then, LocalDateTime.now()) >= 60) {
					throw new RuntimeException("KafkaConsumer is not ready in 60sec.");
				}
			}

			if (this.logger.isDebugEnabled()) {
				this.logger.debug("started " + this.beanName + "...");
			}
			while (!this.stopped.get()) {
				this.started.set(true);

				if (this.subscribers.get().length > 0) {
					this.logger.info("POLL-" + this.groupid + " to " + this.subscribers.get().length + " subscribers");
					ConsumerRecords<K, T> poll = kafkaConsumer.poll(Duration.ofSeconds(10L));

					if (this.logger.isDebugEnabled()) {
						this.logger.debug("poll.count: " + poll.count());
					}

					poll.forEach(r -> {

						if (this.logger.isTraceEnabled()) {
							this.logger.trace("polled:" + r);
						}

						for (PublisherWrapper<T, K> pd : this.subscribers.get()) {
							pd.onNext(r.value());
						}
					});
					kafkaConsumer.commitSync();
				}
			}
			kafkaConsumer.unsubscribe();
			this.started.set(false);
		}
	}

	/**
	 * Getter to message class.
	 *
	 * @return message class
	 */
	public Class<T> getClazz() {
		return this.clazz;
	}

	/**
	 * Getter to message key class.
	 *
	 * @return message key class
	 */
	public Class<K> getClazzKey() {
		return this.clazzKey;
	}

	/**
	 * Getter to is the polling stopped.
	 *
	 * @return stopped
	 */
	public AtomicBoolean getStopped() {
		return this.stopped;
	}

	/**
	 * Getter to is the polling started.
	 *
	 * @return started
	 */
	public AtomicBoolean getStarted() {
		return this.started;
	}

}
