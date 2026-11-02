/*
 * Copyright 2016-present the original author or authors.
 */

package org.springframework.kafka.support.converter;

import java.util.Map;

import org.jspecify.annotations.Nullable;

import org.springframework.kafka.support.JavaUtils;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.KafkaUtils;

/**
 * A top level interface for message converters.
 *
 * @author Gary Russell
 * @since 1.1
 *
 */
public interface MessageConverter {

	/**
	 * Get the thread bound group id.
	 * @return the group id.
	 */
	@Nullable
	static String getGroupId() {
		return KafkaUtils.getConsumerGroupId();
	}

	/**
	 * Set up the common headers.
	 * @param acknowledgment the acknowledgment (can be Acknowledgment or ShareAcknowledgment).
	 * @param consumer the consumer (can be Consumer or ShareConsumer).
	 * @param rawHeaders the raw headers map.
	 * @param theKey the key.
	 * @param topic the topic.
	 * @param partition the partition.
	 * @param offset the offset.
	 * @param timestampType the timestamp type.
	 * @param timestamp the timestamp.
	 */
	@SuppressWarnings("NullAway") // Dataflow analysis limitation
	default void commonHeaders(@Nullable Object acknowledgment, @Nullable Object consumer, Map<String, Object> rawHeaders,
			@Nullable Object theKey, Object topic, Object partition, Object offset,
			@Nullable Object timestampType, Object timestamp) {

		rawHeaders.put(KafkaHeaders.RECEIVED_TOPIC, topic);
		rawHeaders.put(KafkaHeaders.RECEIVED_PARTITION, partition);
		rawHeaders.put(KafkaHeaders.OFFSET, offset);
		rawHeaders.put(KafkaHeaders.TIMESTAMP_TYPE, timestampType);
		rawHeaders.put(KafkaHeaders.RECEIVED_TIMESTAMP, timestamp);
		JavaUtils.INSTANCE
			.acceptIfNotNull(KafkaHeaders.RECEIVED_KEY, theKey, (key, val) -> rawHeaders.put(key, val))
			.acceptIfNotNull(KafkaHeaders.GROUP_ID, MessageConverter.getGroupId(),
					(key, val) -> rawHeaders.put(key, val))
			.acceptIfNotNull(KafkaHeaders.ACKNOWLEDGMENT, acknowledgment, (key, val) -> rawHeaders.put(key, val))
			.acceptIfNotNull(KafkaHeaders.CONSUMER, consumer, (key, val) -> rawHeaders.put(key, val));
	}

}
