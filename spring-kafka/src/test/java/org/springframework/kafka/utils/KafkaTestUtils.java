/*
 * Copyright 2016 the original author or authors.
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
package org.springframework.kafka.utils;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.hamcrest.Description;
import org.hamcrest.DiagnosingMatcher;
import org.hamcrest.Matcher;

import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.test.rule.KafkaEmbedded;

/**
 * @author Gary Russell
 *
 */
public final class KafkaTestUtils {

	private KafkaTestUtils() {}

	public static Map<String, Object> consumerProps(String group, String autoCommit, KafkaEmbedded embeddedKafka) {
		return consumerProps(embeddedKafka.getBrokersAsString(), group, autoCommit);
	}

	public static Map<String, Object> senderProps(KafkaEmbedded embeddedKafka) {
		return senderProps(embeddedKafka.getBrokersAsString());
	}

	public static Map<String, Object> consumerProps(String brokers, String group, String autoCommit) {
		Map<String, Object> props = new HashMap<>();
		props.put("bootstrap.servers", brokers);
		props.put("group.id", group);
		props.put("enable.auto.commit", autoCommit);
		props.put("auto.commit.interval.ms", "100");
		props.put("session.timeout.ms", "15000");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		return props;
	}

	public static Map<String, Object> senderProps(String brokers) {
		Map<String, Object> props = new HashMap<>();
		props.put("bootstrap.servers", brokers);
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		return props;
	}

	public static void waitForAssignment(ConcurrentMessageListenerContainer<Integer, String> container, int partitions)
			throws Exception {
		List<KafkaMessageListenerContainer<Integer, String>> containers = container.getContainers();
		int n = 0;
		int count = 0;
		while (n++ < 600 && count < partitions) {
			count = 0;
			for (KafkaMessageListenerContainer<Integer, String> aContainer : containers) {
				if (aContainer.getAssignedPartitions() != null) {
					count += aContainer.getAssignedPartitions().size();
				}
			}
			if (count < partitions) {
				Thread.sleep(100);
			}
		}
		assertThat(count, equalTo(partitions));
	}

	public static void waitForAssignment(KafkaMessageListenerContainer<Integer, String> container, int partitions)
			throws Exception {
		int n = 0;
		int count = 0;
		while (n++ < 600 && count < partitions) {
			count = 0;
			if (container.getAssignedPartitions() != null) {
				count = container.getAssignedPartitions().size();
			}
			if (count < partitions) {
				Thread.sleep(100);
			}
		}
		assertThat(count, equalTo(partitions));
	}

	public static <K> Matcher<ConsumerRecord<K, ?>> hasKey(K key) {
		return new ConsumerRecordKeyMatcher<K>(key);
	}

	public static <V> Matcher<ConsumerRecord<?, V>> hasValue(V payload) {
		return new ConsumerRecordPayloadMatcher<V>(payload);
	}

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
			boolean matches = record != null && record.key().equals(this.key);
			if (!matches) {
				mismatchDescription.appendText("is ").appendText(record.toString());
			}
			return matches;
		}

	}

	public static class ConsumerRecordPayloadMatcher<V> extends DiagnosingMatcher<ConsumerRecord<?, V>> {

		private final V payload;

		public ConsumerRecordPayloadMatcher(V payload) {
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
				mismatchDescription.appendText("is ").appendText(record.toString());
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
