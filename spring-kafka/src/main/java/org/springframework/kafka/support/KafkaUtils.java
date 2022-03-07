/*
 * Copyright 2018-2022 the original author or authors.
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

package org.springframework.kafka.support;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.function.Function;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import org.springframework.messaging.Message;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

/**
 * Utility methods.
 *
 * @author Gary Russell
 *
 * @since 2.2
 *
 */
public final class KafkaUtils {

	private static Function<ProducerRecord<?, ?>, String> prFormatter = rec -> rec.toString();

	private static Function<ConsumerRecord<?, ?>, String> crFormatter =
			rec -> rec.topic() + "-" + rec.partition() + "@" + rec.offset();

	/**
	 * True if micrometer is on the class path.
	 */
	public static final boolean MICROMETER_PRESENT = ClassUtils.isPresent(
			"io.micrometer.core.instrument.MeterRegistry", KafkaUtils.class.getClassLoader());

	private static final ThreadLocal<String> GROUP_IDS = new ThreadLocal<>();

	/**
	 * Return true if the method return type is {@link Message} or
	 * {@code Collection<Message<?>>}.
	 * @param method the method.
	 * @return true if it returns message(s).
	 */
	public static boolean returnTypeMessageOrCollectionOf(Method method) {
		Type returnType = method.getGenericReturnType();
		if (returnType.equals(Message.class)) {
			return true;
		}
		if (returnType instanceof ParameterizedType) {
			ParameterizedType prt = (ParameterizedType) returnType;
			Type rawType = prt.getRawType();
			if (rawType.equals(Message.class)) {
				return true;
			}
			if (rawType.equals(Collection.class)) {
				Type collectionType = prt.getActualTypeArguments()[0];
				if (collectionType.equals(Message.class)) {
					return true;
				}
				return collectionType instanceof ParameterizedType
						&& ((ParameterizedType) collectionType).getRawType().equals(Message.class);
			}
		}
		return false;

	}

	/**
	 * Set the group id for the consumer bound to this thread.
	 * @param groupId the group id.
	 * @since 2.3
	 */
	public static void setConsumerGroupId(String groupId) {
		KafkaUtils.GROUP_IDS.set(groupId);
	}

	/**
	 * Get the group id for the consumer bound to this thread.
	 * @return the group id.
	 * @since 2.3
	 */
	public static String getConsumerGroupId() {
		return KafkaUtils.GROUP_IDS.get();
	}

	/**
	 * Clear the group id for the consumer bound to this thread.
	 * @since 2.3
	 */
	public static void clearConsumerGroupId() {
		KafkaUtils.GROUP_IDS.remove();
	}

	/**
	 * Return the timeout to use when sending records. If the
	 * {@link ProducerConfig#DELIVERY_TIMEOUT_MS_CONFIG} is not configured, or is not a
	 * number or a String that can be parsed as a long, the {@link ProducerConfig} default
	 * value (plus the buffer) is used.
	 * @param producerProps the producer properties.
	 * @param buffer a buffer to add to the configured
	 * {@link ProducerConfig#DELIVERY_TIMEOUT_MS_CONFIG} to prevent timing out before the
	 * Kafka producer.
	 * @param min a minimum value to apply after adding the buffer to the configured
	 * timeout.
	 * @return the timeout to use.
	 * @since 2.7
	 */
	public static Duration determineSendTimeout(Map<String, Object> producerProps, long buffer, long min) {
		Object dt = producerProps.get(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG);
		if (dt instanceof Number) {
			return Duration.ofMillis(Math.max(((Number) dt).longValue() + buffer, min));
		}
		else if (dt instanceof String) {
			try {
				return Duration.ofMillis(Math.max(Long.parseLong((String) dt) + buffer, min));
			}
			catch (@SuppressWarnings("unused") NumberFormatException ex) {
			}
		}
		return Duration.ofMillis(Math.max(
				((Integer) ProducerConfig.configDef().defaultValues()
						.get(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG)).longValue() + buffer,
				min));
	}

	/**
	 * Set a formatter for logging {@link ConsumerRecord}s; default is
	 * {@code topic-partition@offset}.
	 * @param formatter a function to format the record as a String
	 * @since 2.7.12
	 */
	public static void setConsumerRecordFormatter(Function<ConsumerRecord<?, ?>, String> formatter) {
		Assert.notNull(formatter, "'formatter' cannot be null");
		crFormatter = formatter;
	}

	/**
	 * Set a formatter for logging {@link ProducerRecord}s; default is
	 * {@link ProducerRecord#toString()}.
	 * @param formatter a function to format the record as a String
	 * @since 2.7.12
	 */
	public static void setProducerRecordFormatter(Function<ProducerRecord<?, ?>, String> formatter) {
		Assert.notNull(formatter, "'formatter' cannot be null");
		prFormatter = formatter;
	}

	/**
	 * Format the {@link ConsumerRecord} for logging; default
	 * {@code topic-partition@offset}.
	 * @param record the record to format.
	 * @return the formatted String.
	 * @since 2.7.12
	 */
	public static String format(ConsumerRecord<?, ?> record) {
		return crFormatter.apply(record);
	}

	/**
	 * Format the {@link ConsumerRecord} for logging; default
	 * {@code topic-partition@offset}. Provided for backwards compatibility only.
	 * @param record the record to format.
	 * @param full use {@link ConsumerRecord#toString()}.
	 * @return the formatted String.
	 * @since 2.7.12
	 * @deprecated in favor of {@link #format(ConsumerRecord)}.
	 */
	@Deprecated
	public static String format(ConsumerRecord<?, ?> record, boolean full) {
		if (full) {
			return record.toString();
		}
		return crFormatter.apply(record);
	}

	/**
	 * Format the {@link ProducerRecord} for logging; default
	 * {@link ProducerRecord}{@link #toString()}.
	 * @param record the record to format.
	 * @return the formatted String.
	 * @since 2.7.12
	 */
	public static String format(ProducerRecord<?, ?> record) {
		return prFormatter.apply(record);
	}

	private KafkaUtils() {
	}

}
