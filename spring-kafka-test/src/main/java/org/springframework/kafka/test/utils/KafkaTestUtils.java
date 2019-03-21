/*
 * Copyright 2016 the original author or authors.
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

package org.springframework.kafka.test.utils;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import org.springframework.beans.DirectFieldAccessor;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.util.Assert;

/**
 * Kafka testing utilities.
 *
 * @author Gary Russell
 * @author Hugo Wood
 */
public final class KafkaTestUtils {

	private static final Log logger = LogFactory.getLog(KafkaTestUtils.class);

	private KafkaTestUtils() {
		// private ctor
	}

	/**
	 * Set up test properties for an {@code <Integer, String>} consumer.
	 * @param group the group id.
	 * @param autoCommit the auto commit.
	 * @param embeddedKafka a {@link KafkaEmbedded} instance.
	 * @return the properties.
	 */
	public static Map<String, Object> consumerProps(String group, String autoCommit, KafkaEmbedded embeddedKafka) {
		return consumerProps(embeddedKafka.getBrokersAsString(), group, autoCommit);
	}

	/**
	 * Set up test properties for an {@code <Integer, String>} producer.
	 * @param embeddedKafka a {@link KafkaEmbedded} instance.
	 * @return the properties.
	 */
	public static Map<String, Object> producerProps(KafkaEmbedded embeddedKafka) {
		return senderProps(embeddedKafka.getBrokersAsString());
	}

	/**
	 * Set up test properties for an {@code <Integer, String>} consumer.
	 * @param brokers the bootstrapServers property.
	 * @param group the group id.
	 * @param autoCommit the auto commit.
	 * @return the properties.
	 */
	public static Map<String, Object> consumerProps(String brokers, String group, String autoCommit) {
		Map<String, Object> props = new HashMap<>();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, group);
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, autoCommit);
		props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "10");
		props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 60000);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		return props;
	}

	/**
	 * Set up test properties for an {@code <Integer, String>} producer.
	 * @param brokers the bootstrapServers property.
	 * @return the properties.
	 */
	public static Map<String, Object> senderProps(String brokers) {
		Map<String, Object> props = new HashMap<>();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
		props.put(ProducerConfig.RETRIES_CONFIG, 0);
		props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
		props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
		props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		return props;
	}

	/**
	 * Poll the consumer, expecting a single record for the specified topic.
	 * @param consumer the consumer.
	 * @param topic the topic.
	 * @param <K> the key type.
	 * @param <V> the value type.
	 * @return the record.
	 * @throws org.junit.ComparisonFailure if exactly one record is not received.
	 * @see #getSingleRecord(Consumer, String, long)
	 */
	public static <K, V> ConsumerRecord<K, V> getSingleRecord(Consumer<K, V> consumer, String topic) {
		return getSingleRecord(consumer, topic, 60000);
	}

	/**
	 * Poll the consumer, expecting a single record for the specified topic.
	 * @param consumer the consumer.
	 * @param topic the topic.
	 * @param timeout max time in milliseconds to wait for records; forwarded to {@link Consumer#poll(long)}.
	 * @param <K> the key type.
	 * @param <V> the value type.
	 * @return the record.
	 * @throws org.junit.ComparisonFailure if exactly one record is not received.
	 * @since 2.0
	 */
	public static <K, V> ConsumerRecord<K, V> getSingleRecord(Consumer<K, V> consumer, String topic, long timeout) {
		ConsumerRecords<K, V> received = getRecords(consumer, timeout);
		assertThat(received.count()).as("Incorrect results returned", received.count()).isEqualTo(1);
		return received.records(topic).iterator().next();
	}

	/**
	 * Poll the consumer for records.
	 * @param consumer the consumer.
	 * @param <K> the key type.
	 * @param <V> the value type.
	 * @return the records.
	 * @see #getRecords(Consumer, long)
	 */
	public static <K, V> ConsumerRecords<K, V> getRecords(Consumer<K, V> consumer) {
		return getRecords(consumer, 60000);
	}

	/**
	 * Poll the consumer for records.
	 * @param consumer the consumer.
	 * @param timeout max time in milliseconds to wait for records; forwarded to {@link Consumer#poll(long)}.
	 * @param <K> the key type.
	 * @param <V> the value type.
	 * @return the records.
	 * @since 2.0
	 */
	public static <K, V> ConsumerRecords<K, V> getRecords(Consumer<K, V> consumer, long timeout) {
		logger.debug("Polling...");
		ConsumerRecords<K, V> received = consumer.poll(timeout);
		if (logger.isDebugEnabled()) {
			logger.debug("Received: " + received.count());
		}
		assertThat(received).as("null received from consumer.poll()").isNotNull();
		return received;
	}

	/**
	 * Uses nested {@link DirectFieldAccessor}s to obtain a property using dotted notation to traverse fields; e.g.
	 * "foo.bar.baz" will obtain a reference to the baz field of the bar field of foo. Adopted from Spring Integration.
	 * @param root The object.
	 * @param propertyPath The path.
	 * @return The field.
	 */
	public static Object getPropertyValue(Object root, String propertyPath) {
		Object value = null;
		DirectFieldAccessor accessor = new DirectFieldAccessor(root);
		String[] tokens = propertyPath.split("\\.");
		for (int i = 0; i < tokens.length; i++) {
			value = accessor.getPropertyValue(tokens[i]);
			if (value != null) {
				accessor = new DirectFieldAccessor(value);
			}
			else if (i == tokens.length - 1) {
				return null;
			}
			else {
				throw new IllegalArgumentException("intermediate property '" + tokens[i] + "' is null");
			}
		}
		return value;
	}

	/**
	 * A typed version of {@link #getPropertyValue(Object, String)}.
	 * @param root the object.
	 * @param propertyPath the path.
	 * @param type the type to cast the object to.
	 * @param <T> the type.
	 * @return the field value.
	 * @see #getPropertyValue(Object, String)
	 */
	@SuppressWarnings("unchecked")
	public static <T> T getPropertyValue(Object root, String propertyPath, Class<T> type) {
		Object value = getPropertyValue(root, propertyPath);
		if (value != null) {
			Assert.isAssignable(type, value.getClass());
		}
		return (T) value;
	}

}
