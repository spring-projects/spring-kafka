/*
 * Copyright 2016-2017 the original author or authors.
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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.Serializer;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.context.Lifecycle;

/**
 * The {@link ProducerFactory} implementation for the {@code singleton} shared {@link Producer}
 * instance.
 * <p>
 * This implementation will produce a new {@link Producer} instance
 * for provided {@link Map} {@code configs} and optional {@link Serializer} {@code keySerializer},
 * {@code valueSerializer} implementations on each {@link #createProducer()}
 * invocation.
 * <p>
 * The {@link Producer} instance is freed from the external {@link Producer#close()} invocation
 * with the internal wrapper. The real {@link Producer#close()} is called on the target
 * {@link Producer} during the {@link Lifecycle#stop()} or {@link DisposableBean#destroy()}.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Gary Russell
 * @author Murali Reddy
 */
public class DefaultKafkaProducerFactory<K, V> implements ProducerFactory<K, V>, Lifecycle, DisposableBean {

	private static final Log logger = LogFactory.getLog(DefaultKafkaProducerFactory.class);

	private final Map<String, Object> configs;

	private volatile CloseSafeProducer<K, V> producer;

	private Serializer<K> keySerializer;

	private Serializer<V> valueSerializer;

	private volatile boolean running;

	public DefaultKafkaProducerFactory(Map<String, Object> configs) {
		this(configs, null, null);
	}

	public DefaultKafkaProducerFactory(Map<String, Object> configs, Serializer<K> keySerializer,
			Serializer<V> valueSerializer) {
		this.configs = new HashMap<>(configs);
		this.keySerializer = keySerializer;
		this.valueSerializer = valueSerializer;
	}

	public void setKeySerializer(Serializer<K> keySerializer) {
		this.keySerializer = keySerializer;
	}

	public void setValueSerializer(Serializer<V> valueSerializer) {
		this.valueSerializer = valueSerializer;
	}

	/**
	 * Return an unmodifiable reference to the configuration map for this factory.
	 * Useful for cloning to make a similar factory.
	 * @return the configs.
	 * @since 2.0
	 */
	public Map<String, Object> getConfigurationProperties() {
		return Collections.unmodifiableMap(this.configs);
	}

	@Override
	public void destroy() throws Exception { //NOSONAR
		CloseSafeProducer<K, V> producer = this.producer;
		this.producer = null;
		if (producer != null) {
			producer.delegate.close();
		}
	}


	@Override
	public void start() {
		this.running = true;
	}


	@Override
	public void stop() {
		try {
			destroy();
		}
		catch (Exception e) {
			logger.error("Exception while stopping producer", e);
		}
	}


	@Override
	public boolean isRunning() {
		return this.running;
	}

	@Override
	public Producer<K, V> createProducer() {
		if (this.producer == null) {
			synchronized (this) {
				if (this.producer == null) {
					this.producer = new CloseSafeProducer<K, V>(createKafkaProducer());
				}
			}
		}
		return this.producer;
	}

	protected KafkaProducer<K, V> createKafkaProducer() {
		return new KafkaProducer<K, V>(this.configs, this.keySerializer, this.valueSerializer);
	}

	private static class CloseSafeProducer<K, V> implements Producer<K, V> {

		private final Producer<K, V> delegate;

		CloseSafeProducer(Producer<K, V> delegate) {
			this.delegate = delegate;
		}

		@Override
		public Future<RecordMetadata> send(ProducerRecord<K, V> record) {
			return this.delegate.send(record);
		}

		@Override
		public Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback) {
			return this.delegate.send(record, callback);
		}

		@Override
		public void flush() {
			this.delegate.flush();
		}

		@Override
		public List<PartitionInfo> partitionsFor(String topic) {
			return this.delegate.partitionsFor(topic);
		}

		@Override
		public Map<MetricName, ? extends Metric> metrics() {
			return this.delegate.metrics();
		}

		@Override
		public void close() {
		}

		@Override
		public void close(long timeout, TimeUnit unit) {
		}

	}

}
