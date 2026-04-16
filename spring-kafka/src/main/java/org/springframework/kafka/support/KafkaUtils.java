/*
 * Copyright 2018-present the original author or authors.
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.jspecify.annotations.Nullable;

import org.springframework.messaging.Message;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

/**
 * Utility methods.
 *
 * @author Gary Russell
 * @author Wang ZhiYang
 * @author Soby Chacko
 *
 * @since 2.2
 *
 */
public final class KafkaUtils {

	/**
	 * Header name for deserialization exceptions.
	 * @since 3.0.15
	 */
	public static final String DESERIALIZER_EXCEPTION_HEADER_PREFIX = "springDeserializerException";

	/**
	 * Header name for deserialization exceptions.
	 * @since 3.0.15
	 */
	public static final String KEY_DESERIALIZER_EXCEPTION_HEADER = DESERIALIZER_EXCEPTION_HEADER_PREFIX + "Key";

	/**
	 * Header name for deserialization exceptions.
	 * @since 3.0.15
	 */
	public static final String VALUE_DESERIALIZER_EXCEPTION_HEADER = DESERIALIZER_EXCEPTION_HEADER_PREFIX + "Value";

	private static Function<ProducerRecord<?, ?>, String> prFormatter = ProducerRecord::toString;

	private static Function<ConsumerRecord<?, ?>, String> crFormatter =
			rec -> rec.topic() + "-" + rec.partition() + "@" + rec.offset();

	/**
	 * True if micrometer is on the class path.
	 */
	public static final boolean MICROMETER_PRESENT = ClassUtils.isPresent(
			"io.micrometer.core.instrument.MeterRegistry", KafkaUtils.class.getClassLoader());

	private static final Map<Thread, String> GROUP_IDS = new ConcurrentHashMap<>();

	/**
	 * Return true if the method return type is {@link Message} or
	 * {@code Collection<Message<?>>}.
	 * @param method the method.
	 * @return true if it returns message(s).
	 */
	public static boolean returnTypeMessageOrCollectionOf(Method method) {
		Type returnType = method.getGenericReturnType();
		if (returnType instanceof ParameterizedType prt) {
			returnType = prt.getRawType();
			if (Collection.class.equals(returnType)) {
				returnType = prt.getActualTypeArguments()[0];
			}
			if (returnType instanceof ParameterizedType pType) {
				returnType = pType.getRawType();
			}
		}
		return Message.class.equals(returnType);
	}

	/**
	 * Set the group id for the consumer bound to this thread.
	 * @param groupId the group id.
	 * @since 2.3
	 */
	public static void setConsumerGroupId(@Nullable String groupId) {
		if (groupId != null) {
			KafkaUtils.GROUP_IDS.put(Thread.currentThread(), groupId);
		}
	}

	/**
	 * Get the group id for the consumer bound to this thread.
	 * @return the group id.
	 * @since 2.3
	 */
	public static @Nullable String getConsumerGroupId() {
		return KafkaUtils.GROUP_IDS.get(Thread.currentThread());
	}

	/**
	 * Clear the group id for the consumer bound to this thread.
	 * @since 2.3
	 */
	public static void clearConsumerGroupId() {
		KafkaUtils.GROUP_IDS.remove(Thread.currentThread());
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
		if (dt instanceof Number number) {
			return Duration.ofMillis(Math.max(number.longValue() + buffer, min));
		}
		else if (dt instanceof String str) {
			try {
				return Duration.ofMillis(Math.max(Long.parseLong(str) + buffer, min));
			}
			catch (@SuppressWarnings("unused") NumberFormatException ex) {
			}
		}
		Integer deliveryTimeoutInMs = (Integer) ProducerConfig.configDef().defaultValues()
				.get(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG);
		return Duration.ofMillis(Math.max(
				deliveryTimeoutInMs == null ? 0 : deliveryTimeoutInMs.longValue() + buffer,
				min));
	}

	/**
	 * Set a formatter for logging {@link ConsumerRecord}s.
	 * @param formatter a function to format the record as a String
	 * @since 2.7.12
	 */
	public static void setConsumerRecordFormatter(Function<ConsumerRecord<?, ?>, String> formatter) {
		Assert.notNull(formatter, "'formatter' cannot be null");
		crFormatter = formatter;
	}

	/**
	 * Set a formatter for logging {@link ProducerRecord}s.
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
