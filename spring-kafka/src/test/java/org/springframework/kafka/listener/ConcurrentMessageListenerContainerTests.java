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

package org.springframework.kafka.listener;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.springframework.beans.DirectFieldAccessor;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.AbstractMessageListenerContainer.AckMode;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer.ContainerOffsetResetStrategy;
import org.springframework.kafka.rule.KafkaEmbedded;

/**
 * @author Gary Russell
 *
 */
public class ConcurrentMessageListenerContainerTests {

	private final Log logger = LogFactory.getLog(this.getClass());

	private static String topic1 = "testTopic1";

	private static String topic2 = "testTopic2";

	private static String topic3 = "testTopic3";

	private static String topic4 = "testTopic4";

	private static String topic5 = "testTopic5";

	private static String topic6 = "testTopic6";

	@ClassRule
	public static KafkaEmbedded embeddedKafka = new KafkaEmbedded(1, true, topic1, topic2, topic3, topic4, topic5,
			topic6);

	@Test
	public void testAutoCommit() throws Exception {
		logger.info("Start auto");
		Map<String, Object> props = consumerProps("test1", "true");
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(props);
		ConcurrentMessageListenerContainer<Integer, String> container =
				new ConcurrentMessageListenerContainer<>(cf, topic1);
		final CountDownLatch latch = new CountDownLatch(4);
		container.setMessageListener(new MessageListener<Integer, String>() {

			@Override
			public void onMessage(ConsumerRecord<Integer, String> message) {
				logger.info("auto: " + message);
				latch.countDown();
			}
		});
		container.setConcurrency(2);
		container.setBeanName("testAuto");
		container.start();
		waitForAssignment(container, embeddedKafka.getPartitionsPerTopic());
		Map<String, Object> senderProps = senderProps();
		ProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf);
		template.setDefaultTopic(topic1);
		template.convertAndSend(0, "foo");
		template.convertAndSend(2, "bar");
		template.convertAndSend(0, "baz");
		template.convertAndSend(2, "qux");
		template.flush();
		assertTrue(latch.await(60, TimeUnit.SECONDS));
		container.stop();
		logger.info("Stop auto");

	}

	@Test
	public void testAfterListenCommit() throws Exception {
		logger.info("Start manual");
		Map<String, Object> props = consumerProps("test2", "false");
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(props);
		ConcurrentMessageListenerContainer<Integer, String> container =
				new ConcurrentMessageListenerContainer<>(cf, topic2);
		final CountDownLatch latch = new CountDownLatch(4);
		container.setMessageListener(new MessageListener<Integer, String>() {

			@Override
			public void onMessage(ConsumerRecord<Integer, String> message) {
				logger.info("manual: " + message);
				latch.countDown();
			}
		});
		container.setConcurrency(2);
		container.setBeanName("testBatch");
		container.start();
		waitForAssignment(container, embeddedKafka.getPartitionsPerTopic());
		Map<String, Object> senderProps = senderProps();
		ProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf);
		template.setDefaultTopic(topic2);
		template.convertAndSend(0, "foo");
		template.convertAndSend(2, "bar");
		template.convertAndSend(0, "baz");
		template.convertAndSend(2, "qux");
		template.flush();
		assertTrue(latch.await(60, TimeUnit.SECONDS));
		container.stop();
		logger.info("Stop manual");
	}

	@Test
	public void testDefinedPartitions() throws Exception {
		logger.info("Start auto parts");
		Map<String, Object> props = consumerProps("test3", "true");
		TopicPartition topic1Partition0 = new TopicPartition(topic3, 0);
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(props);
		ConcurrentMessageListenerContainer<Integer, String> container1 =
				new ConcurrentMessageListenerContainer<>(cf, topic1Partition0);
		final CountDownLatch latch1 = new CountDownLatch(2);
		container1.setMessageListener(new MessageListener<Integer, String>() {

			@Override
			public void onMessage(ConsumerRecord<Integer, String> message) {
				logger.info("auto part: " + message);
				latch1.countDown();
			}
		});
		container1.setBeanName("b1");
		container1.start();
		Thread.sleep(1000);
		TopicPartition topic1Partition1 = new TopicPartition(topic3, 1);
		ConcurrentMessageListenerContainer<Integer, String> container2 =
				new ConcurrentMessageListenerContainer<>(cf, topic1Partition1);
		final CountDownLatch latch2 = new CountDownLatch(2);
		container2.setMessageListener(new MessageListener<Integer, String>() {

			@Override
			public void onMessage(ConsumerRecord<Integer, String> message) {
				logger.info("auto part: " + message);
				latch2.countDown();
			}
		});
		container2.setBeanName("b2");
		container2.start();
		Thread.sleep(1000);
		Map<String, Object> senderProps = senderProps();
		ProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf);
		template.setDefaultTopic(topic3);
		template.convertAndSend(0, "foo");
		template.convertAndSend(2, "bar");
		template.convertAndSend(0, "baz");
		template.convertAndSend(2, "qux");
		template.flush();
		assertTrue(latch1.await(60, TimeUnit.SECONDS));
		container1.stop();
		container2.stop();

		// reset earliest
		ConcurrentMessageListenerContainer<Integer, String> resettingContainer =
				new ConcurrentMessageListenerContainer<>(cf, topic1Partition0, topic1Partition1);
		resettingContainer.setBeanName("b3");
		final CountDownLatch latch3 = new CountDownLatch(4);
		resettingContainer.setMessageListener(new MessageListener<Integer, String>() {

			@Override
			public void onMessage(ConsumerRecord<Integer, String> message) {
				logger.info("auto part e: " + message);
				latch3.countDown();
			}
		});
		resettingContainer.setResetStrategy(ContainerOffsetResetStrategy.EARLIEST);
		resettingContainer.start();
		assertTrue(latch3.await(60, TimeUnit.SECONDS));
		resettingContainer.stop();
		assertThat(latch3.getCount(), equalTo(0L));

		// reset minusone
		resettingContainer = new ConcurrentMessageListenerContainer<>(cf, topic1Partition0, topic1Partition1);
		resettingContainer.setBeanName("b4");
		final CountDownLatch latch4 = new CountDownLatch(2);
		final AtomicReference<String> receivedMessage = new AtomicReference<>();
		resettingContainer.setMessageListener(new MessageListener<Integer, String>() {

			@Override
			public void onMessage(ConsumerRecord<Integer, String> message) {
				logger.info("auto part -1: " + message);
				receivedMessage.set(message.value());
				latch4.countDown();
			}
		});
		resettingContainer.setResetStrategy(ContainerOffsetResetStrategy.RECENT);
		resettingContainer.start();
		assertTrue(latch4.await(60, TimeUnit.SECONDS));
		resettingContainer.stop();
		assertThat(receivedMessage.get(), anyOf(equalTo("baz"), equalTo("qux")));
		assertThat(latch4.getCount(), equalTo(0L));

		logger.info("Stop auto parts");
	}

	@Test
	public void testManualCommit() throws Exception {
		testManualCommitGuts(AckMode.MANUAL, topic4);
		testManualCommitGuts(AckMode.MANUAL_IMMEDIATE, topic5);
		// to be sure the commits worked ok so run the tests again and the second tests start at the committed offset.
		testManualCommitGuts(AckMode.MANUAL, topic4);
		testManualCommitGuts(AckMode.MANUAL_IMMEDIATE, topic5);
	}

	private void testManualCommitGuts(AckMode ackMode, String topic) throws Exception {
		logger.info("Start " + ackMode);
		Map<String, Object> props = consumerProps("test4", "false");
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(props);
		ConcurrentMessageListenerContainer<Integer, String> container =
				new ConcurrentMessageListenerContainer<>(cf, topic);
		final CountDownLatch latch = new CountDownLatch(4);
		container.setMessageListener(new AcknowledgingMessageListener<Integer, String>() {

			@Override
			public void onMessage(ConsumerRecord<Integer, String> message, Acknowledgment ack) {
				logger.info("manual: " + message);
				ack.acknowledge();
				latch.countDown();
			}

		});
		container.setConcurrency(2);
		container.setAckMode(ackMode);
		container.setBeanName("test" + ackMode);
		container.start();
		waitForAssignment(container, embeddedKafka.getPartitionsPerTopic());
		Map<String, Object> senderProps = senderProps();
		ProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf);
		template.setDefaultTopic(topic);
		template.convertAndSend(0, "foo");
		template.convertAndSend(2, "bar");
		template.convertAndSend(0, "baz");
		template.convertAndSend(2, "qux");
		template.flush();
		assertTrue(latch.await(60, TimeUnit.SECONDS));
		container.stop();
		logger.info("Stop " + ackMode);
	}


	@SuppressWarnings("unchecked")
	@Test
	public void testConcurrencyWithPartitions() {
		TopicPartition[] topic1PartitionS = new TopicPartition[]{
				new TopicPartition(topic1, 0),
				new TopicPartition(topic1, 1),
				new TopicPartition(topic1, 2),
				new TopicPartition(topic1, 3),
				new TopicPartition(topic1, 4),
				new TopicPartition(topic1, 5),
				new TopicPartition(topic1, 6)
		};
		ConsumerFactory<Integer, String> cf = mock(ConsumerFactory.class);
		Consumer<Integer, String> consumer = mock(Consumer.class);
		when(cf.createConsumer()).thenReturn(consumer);
		doAnswer(new Answer<ConsumerRecords<Integer, String>>() {

			@Override
			public ConsumerRecords<Integer, String> answer(InvocationOnMock invocation) throws Throwable {
				Thread.sleep(100);
				return null;
			}

		}).when(consumer).poll(anyLong());
		ConcurrentMessageListenerContainer<Integer, String> container =
				new ConcurrentMessageListenerContainer<>(cf, topic1PartitionS);
		container.setMessageListener(new MessageListener<Integer, String>() {

			@Override
			public void onMessage(ConsumerRecord<Integer, String> message) {
			}

		});
		container.setConcurrency(3);
		container.start();
		List<KafkaMessageListenerContainer<Integer, String>> containers =
				(List<KafkaMessageListenerContainer<Integer, String>>) new DirectFieldAccessor(
				container).getPropertyValue("containers");
		assertEquals(3, containers.size());
		for (int i = 0; i < 3; i++) {
			assertEquals(i < 2 ? 2 : 3, ((TopicPartition[]) new DirectFieldAccessor(containers.get(i))
					.getPropertyValue("partitions")).length);
		}
		container.stop();
	}

	@Test
	public void testListenerException() throws Exception {
		logger.info("Start exception");
		Map<String, Object> props = consumerProps("test1", "true");
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(props);
		ConcurrentMessageListenerContainer<Integer, String> container =
				new ConcurrentMessageListenerContainer<>(cf, topic6);
		final CountDownLatch latch = new CountDownLatch(4);
		container.setMessageListener(new MessageListener<Integer, String>() {

			@Override
			public void onMessage(ConsumerRecord<Integer, String> message) {
				logger.info("auto: " + message);
				latch.countDown();
				throw new RuntimeException("intended");
			}
		});
		container.setConcurrency(2);
		container.setBeanName("testException");
		container.start();
		waitForAssignment(container, embeddedKafka.getPartitionsPerTopic());
		Map<String, Object> senderProps = senderProps();
		ProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf);
		template.setDefaultTopic(topic6);
		template.convertAndSend(0, "foo");
		template.convertAndSend(2, "bar");
		template.convertAndSend(0, "baz");
		template.convertAndSend(2, "qux");
		template.flush();
		assertTrue(latch.await(60, TimeUnit.SECONDS));
		container.stop();
		logger.info("Stop exception");

	}

	private Map<String, Object> consumerProps(String group, String autoCommit) {
		Map<String, Object> props = new HashMap<>();
		props.put("bootstrap.servers", embeddedKafka.getBrokersAsString());
//		props.put("bootstrap.servers", "localhost:9092");
		props.put("group.id", group);
		props.put("enable.auto.commit", autoCommit);
		props.put("auto.commit.interval.ms", "100");
		props.put("session.timeout.ms", "15000");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		return props;
	}

	private Map<String, Object> senderProps() {
		Map<String, Object> props = new HashMap<>();
		props.put("bootstrap.servers", embeddedKafka.getBrokersAsString());
//		props.put("bootstrap.servers", "localhost:9092");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		return props;
	}

	private void waitForAssignment(ConcurrentMessageListenerContainer<Integer, String> container, int partitions)
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

}
