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

package org.springframework.kafka.test.hamcrest;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.hamcrest.Description;
import org.hamcrest.DiagnosingMatcher;
import org.hamcrest.Matcher;

/**
 * Hamcrest {@link Matcher}s utilities.
 *
 * @author Gary Russell
 *
 */
public final class KafkaMatchers {

	private KafkaMatchers() {
		// private ctor
	}

	/**
	 * @param key the key
	 * @param <K> the type.
	 * @return a Matcher that matches the key in a consumer record.
	 */
	public static <K> Matcher<ConsumerRecord<K, ?>> hasKey(K key) {
		return new ConsumerRecordKeyMatcher<K>(key);
	}

	/**
	 * @param value the value.
	 * @param <V> the type.
	 * @return a Matcher that matches the value in a consumer record.
	 */
	public static <V> Matcher<ConsumerRecord<?, V>> hasValue(V value) {
		return new ConsumerRecordValueMatcher<V>(value);
	}

	/**
	 * @param partition the partition.
	 * @return a Matcher that matches the partition in a consumer record.
	 */
	public static Matcher<ConsumerRecord<?, ?>> hasPartition(int partition) {
		return new ConsumerRecordPartitionMatcher(partition);
	}


	public static class ConsumerRecordKeyMatcher<K> extends DiagnosingMatcher<ConsumerRecord<K, ?>> {

		private final K key;

		public ConsumerRecordKeyMatcher(K key) {
			this.key = key;
		}

		@Override
		public void describeTo(Description description) {
			description.appendText("a ConsumerRecord with key ").appendText(this.key.toString());
		}

		@Override
		protected boolean matches(Object item, Description mismatchDescription) {
			@SuppressWarnings("unchecked")
			ConsumerRecord<K, Object> record = (ConsumerRecord<K, Object>) item;
			boolean matches = record != null
					&& ((record.key() == null && this.key == null) || record.key().equals(this.key));
			if (!matches) {
				mismatchDescription.appendText("is ").appendValue(record);
			}
			return matches;
		}

	}

	public static class ConsumerRecordValueMatcher<V> extends DiagnosingMatcher<ConsumerRecord<?, V>> {

		private final V payload;

		public ConsumerRecordValueMatcher(V payload) {
			this.payload = payload;
		}

		@Override
		public void describeTo(Description description) {
			description.appendText("a ConsumerRecord with value ").appendText(this.payload.toString());
		}

		@Override
		protected boolean matches(Object item, Description mismatchDescription) {
			@SuppressWarnings("unchecked")
			ConsumerRecord<Object, V> record = (ConsumerRecord<Object, V>) item;
			boolean matches = record != null && record.value().equals(this.payload);
			if (!matches) {
				mismatchDescription.appendText("is ").appendValue(record);
			}
			return matches;
		}

	}

	public static class ConsumerRecordPartitionMatcher extends DiagnosingMatcher<ConsumerRecord<?, ?>> {

		private final int partition;

		public ConsumerRecordPartitionMatcher(int partition) {
			this.partition = partition;
		}

		@Override
		public void describeTo(Description description) {
			description.appendText("a ConsumerRecord with partition ").appendValue(this.partition);
		}

		@Override
		protected boolean matches(Object item, Description mismatchDescription) {
			@SuppressWarnings("unchecked")
			ConsumerRecord<Object, Object> record = (ConsumerRecord<Object, Object>) item;
			boolean matches = record != null && record.partition() == this.partition;
			if (!matches) {
				mismatchDescription.appendText("is ").appendValue(record);
			}
			return matches;
		}

	}

}
