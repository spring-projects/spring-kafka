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

package org.springframework.kafka.test.assertj;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.record.TimestampType;
import org.assertj.core.api.Condition;

/**
 * AssertJ custom {@link Condition}s.
 *
 * @author Artem Bilan
 * @author Gary Russell
 * @author Biju Kunjummen
 */
public final class KafkaConditions {

	private KafkaConditions() {
		// private ctor
	}

	/**
	 * @param key the key
	 * @param <K> the type.
	 * @return a Condition that matches the key in a consumer record.
	 */
	public static <K> Condition<ConsumerRecord<K, ?>> key(K key) {
		return new ConsumerRecordKeyCondition<>(key);
	}

	/**
	 * @param value the value.
	 * @param <V> the type.
	 * @return a Condition that matches the value in a consumer record.
	 */
	public static <V> Condition<ConsumerRecord<?, V>> value(V value) {
		return new ConsumerRecordValueCondition<>(value);
	}

	/**
	 * @param value the timestamp.
	 * @return a Condition that matches the timestamp value in a consumer record.
	 * @since 1.3
	 */
	public static Condition<ConsumerRecord<?, ?>> timestamp(long value) {
		return timestamp(TimestampType.CREATE_TIME, value);
	}

	/**
	 * @param type the type of timestamp
	 * @param value the timestamp.
	 * @return a Condition that matches the timestamp value in a consumer record.
	 * @since 1.3
	 */
	public static Condition<ConsumerRecord<?, ?>> timestamp(TimestampType type, long value) {
		return new ConsumerRecordTimestampCondition(type, value);
	}

	/**
	 * @param partition the partition.
	 * @return a Condition that matches the partition in a consumer record.
	 */
	public static Condition<ConsumerRecord<?, ?>> partition(int partition) {
		return new ConsumerRecordPartitionCondition(partition);
	}


	public static class ConsumerRecordKeyCondition<K> extends Condition<ConsumerRecord<K, ?>> {

		private final K key;

		public ConsumerRecordKeyCondition(K key) {
			super("a ConsumerRecord with 'key' " + key);
			this.key = key;
		}

		@Override
		public boolean matches(ConsumerRecord<K, ?> value) {
			return value != null && ((value.key() == null && this.key == null) || value.key().equals(this.key));
		}

	}

	public static class ConsumerRecordValueCondition<V> extends Condition<ConsumerRecord<?, V>> {

		private final V payload;

		public ConsumerRecordValueCondition(V payload) {
			super("a ConsumerRecord with 'value' " + payload);
			this.payload = payload;
		}

		@Override
		public boolean matches(ConsumerRecord<?, V> value) {
			return value != null && value.value().equals(this.payload);
		}

	}

	public static class ConsumerRecordTimestampCondition extends Condition<ConsumerRecord<?, ?>> {

		private final TimestampType type;

		private final long ts;

		public ConsumerRecordTimestampCondition(TimestampType type, long ts) {
			super("a ConsumerRecord with timestamp of type: " + type + " and timestamp: " + ts);
			this.type = type;
			this.ts = ts;
		}

		@Override
		public boolean matches(ConsumerRecord<?, ?> value) {
			return value != null &&
					(value.timestampType() == this.type && value.timestamp() == this.ts);
		}

	}

	public static class ConsumerRecordPartitionCondition extends Condition<ConsumerRecord<?, ?>> {

		private final int partition;

		public ConsumerRecordPartitionCondition(int partition) {
			super("a ConsumerRecord with 'partition' " + partition);
			this.partition = partition;
		}

		@Override
		public boolean matches(ConsumerRecord<?, ?> value) {
			return value != null && value.partition() == this.partition;
		}

	}

}
