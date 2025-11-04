/*
 * Copyright 2020 the original author or authors.
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

package org.springframework.kafka.listener.adapter;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.record.TimestampType;

/**
 * Used to provide a listener method argument when the user supplies such a parameter.
 * Delegates to {@link RecordMetadata} (which is final, hence no subclass) for all except
 * timestamp type.
 *
 * @author Gary Russell
 * @since 2.5
 *
 */
public class ConsumerRecordMetadata {

	private final RecordMetadata delegate;

	private final TimestampType timestampType;

	public ConsumerRecordMetadata(RecordMetadata delegate, TimestampType timestampType) {
		this.delegate = delegate;
		this.timestampType = timestampType;
	}

	/**
	 * Return true if the offset is valid.
	 * @return true if the offset is valid.
	 */
	public boolean hasOffset() {
		return this.delegate.hasOffset();
	}

	/**
	 * Return the offset of the record in the topic partition.
	 * @return the offset.
	 */
	public long offset() {
		return this.delegate.offset();
	}

	/**
	 * Return true if the timestamp is valid.
	 * @return true if the timestamp is valid.
	 */
	public boolean hasTimestamp() {
		return this.delegate.hasTimestamp();
	}

	/**
	 * Return the timestamp of the record.
	 * @return the timestamp.
	 */
	public long timestamp() {
		return this.delegate.timestamp();
	}

	/**
	 * Return the size of the serialized, uncompressed key in bytes.
	 * @return the size of the serialized key.
	 */
	public int serializedKeySize() {
		return this.delegate.serializedKeySize();
	}

	/**
	 * Return the size of the serialized, uncompressed value in bytes.
	 * @return the size of the serialized value.
	 */
	public int serializedValueSize() {
		return this.delegate.serializedValueSize();
	}

	/**
	 * Return the topic name the record was appended to.
	 * @return the topic name.
	 */
	public String topic() {
		return this.delegate.topic();
	}

	/**
	 * Return the partition the record was sent to.
	 * @return the partition.
	 */
	public int partition() {
		return this.delegate.partition();
	}

	/**
	 * Return the timestamp type for this record.
	 * @return the timestamp type.
	 */
	public TimestampType timestampType() {
		return this.timestampType;
	}

	@Override
	public int hashCode() {
		return this.delegate.hashCode() + this.timestampType.name().hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof ConsumerRecordMetadata)) {
			return false;
		}
		ConsumerRecordMetadata crm = (ConsumerRecordMetadata) obj;
		return this.delegate.equals(crm.delegate)
				&& this.timestampType.equals(crm.timestampType());
	}

	@Override
	public String toString() {
		return this.delegate.toString();
	}

}
