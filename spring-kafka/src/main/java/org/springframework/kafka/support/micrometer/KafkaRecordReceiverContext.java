/*
 * Copyright 2022-present the original author or authors.
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

package org.springframework.kafka.support.micrometer;

import java.nio.charset.StandardCharsets;
import java.util.function.Supplier;

import io.micrometer.observation.transport.ReceiverContext;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.jspecify.annotations.Nullable;

/**
 * {@link ReceiverContext} for {@link ConsumerRecord}s.
 *
 * @author Gary Russell
 * @author Christian Mergenthaler
 * @author Wang Zhiyang
 *
 * @since 3.0
 *
 */
public class KafkaRecordReceiverContext extends ReceiverContext<ConsumerRecord<?, ?>> {

	private final String listenerId;

	private final @Nullable String clientId;

	private final @Nullable String groupId;

	private final ConsumerRecord<?, ?> record;

	/**
	 * Construct a kafka record receiver context.
	 * @param record 		the consumer record.
	 * @param listenerId	the container listener id.
	 * @param clusterId		the kafka cluster id.
	 */
	public KafkaRecordReceiverContext(ConsumerRecord<?, ?> record, String listenerId, Supplier<String> clusterId) {
		this(record, listenerId, null, null, clusterId);
	}

	/**
	 * Construct a kafka record receiver context.
	 * @param record 		the consumer record.
	 * @param listenerId	the container listener id.
	 * @param clientId		the kafka client id.
	 * @param groupId		the consumer group id.
	 * @param clusterId		the kafka cluster id.
	 * @since 3.2
	 */
	@SuppressWarnings("this-escape")
	public KafkaRecordReceiverContext(ConsumerRecord<?, ?> record, String listenerId, @Nullable String clientId,
			@Nullable String groupId, Supplier<String> clusterId) {
		super((carrier, key) -> {
			Header header = carrier.headers().lastHeader(key);
			if (header == null || header.value() == null) {
				return null;
			}
			return new String(header.value(), StandardCharsets.UTF_8);
		});
		setCarrier(record);
		this.record = record;
		this.listenerId = listenerId;
		this.clientId = clientId;
		this.groupId = groupId;
		String cluster = clusterId.get();
		setRemoteServiceName("Apache Kafka" + (cluster != null ? ": " + cluster : ""));
	}

	/**
	 * Return the listener id.
	 * @return the listener id.
	 */
	public String getListenerId() {
		return this.listenerId;
	}

	/**
	 * Return the consumer group id.
	 * @return the consumer group id.
	 * @since 3.2
	 */
	public @Nullable String getGroupId() {
		return this.groupId;
	}

	/**
	 * Return the client id.
	 * @return the client id.
	 * @since 3.2
	 */
	@Nullable
	public String getClientId() {
		return this.clientId;
	}

	/**
	 * Return the source topic.
	 * @return the source.
	 */
	public String getSource() {
		return this.record.topic();
	}

	/**
	 * Return the consumer record.
	 * @return the record.
	 * @since 3.0.6
	 */
	public ConsumerRecord<?, ?> getRecord() {
		return this.record;
	}

	/**
	 * Return the partition.
	 * @return the partition.
	 * @since 3.2
	 */
	public String getPartition() {
		return Integer.toString(this.record.partition());
	}

	/**
	 * Return the offset.
	 * @return the offset.
	 * @since 3.2
	 */
	public String getOffset() {
		return Long.toString(this.record.offset());
	}

}
