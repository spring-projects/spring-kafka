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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.springframework.beans.BeanUtils;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.AbstractMessageListenerContainer.AckMode;
import org.springframework.kafka.listener.config.ContainerProperties;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.kafka.test.utils.KafkaTestUtils;

/**
 * @author Gary Russell
 * @author Artem Bilan
 * @author Jerome Mirc
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

	private static String topic7 = "testTopic7";

	private static String topic8 = "testTopic8";

	@ClassRule
	public static KafkaEmbedded embeddedKafka = new KafkaEmbedded(1, true, topic1, topic2, topic3, topic4, topic5,
			topic6, topic7, topic8);

	@Test
	public void testAutoCommit() throws Exception {
		this.logger.info("Start auto");
		Map<String, Object> props = KafkaTestUtils.consumerProps("test1", "true", embeddedKafka);
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<Integer, String>(props);
		ContainerProperties containerProps = new ContainerProperties(topic1);
		ConcurrentMessageListenerContainer<Integer, String> container =
				new ConcurrentMessageListenerContainer<>(cf, containerProps);
		final CountDownLatch latch = new CountDownLatch(4);
		final List<String> listenerThreadNames = new ArrayList<>();
		containerProps.setMessageListener(new MessageListener<Integer, String>() {
			@Override
			public void onMessage(ConsumerRecord<Integer, String> message) {
				ConcurrentMessageListenerContainerTests.this.logger.info("auto: " + message);
				listenerThreadNames.add(Thread.currentThread().getName());
				latch.countDown();
			}
		});
		container.setConcurrency(2);
		container.setBeanName("testAuto");
		container.start();
		ContainerTestUtils.waitForAssignment(container, embeddedKafka.getPartitionsPerTopic());
		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		ProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<Integer, String>(senderProps);
		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf);
		template.setDefaultTopic(topic1);
		template.sendDefault(0, "foo");
		template.sendDefault(2, "bar");
		template.sendDefault(0, "baz");
		template.sendDefault(2, "qux");
		template.flush();
		assertThat(latch.await(60, TimeUnit.SECONDS)).isTrue();
		assertThat(listenerThreadNames).allMatch(threadName -> threadName.contains("-consumer-"));
		container.stop();
		this.logger.info("Stop auto");
	}

	@Test
	public void testAutoCommitWithRebalanceListener() throws Exception {
		this.logger.info("Start auto");
		Map<String, Object> props = KafkaTestUtils.consumerProps("test10", "true", embeddedKafka);
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<Integer, String>(props);
		ContainerProperties containerProps = new ContainerProperties(topic1);
		ConcurrentMessageListenerContainer<Integer, String> container =
				new ConcurrentMessageListenerContainer<>(cf, containerProps);
		final CountDownLatch latch = new CountDownLatch(4);
		final List<String> listenerThreadNames = new ArrayList<>();
		containerProps.setMessageListener(new MessageListener<Integer, String>() {

			@Override
			public void onMessage(ConsumerRecord<Integer, String> message) {
				ConcurrentMessageListenerContainerTests.this.logger.info("auto: " + message);
				listenerThreadNames.add(Thread.currentThread().getName());
				latch.countDown();
			}
		});
		final CountDownLatch rebalancePartitionsAssignedLatch = new CountDownLatch(2);
		final CountDownLatch rebalancePartitionsRevokedLatch = new CountDownLatch(2);
		containerProps.setConsumerRebalanceListener(new ConsumerRebalanceListener() {

			@Override
			public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
				ConcurrentMessageListenerContainerTests.this.logger.info("In test, partitions revoked:" + partitions);
				rebalancePartitionsRevokedLatch.countDown();
			}

			@Override
			public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
				ConcurrentMessageListenerContainerTests.this.logger.info("In test, partitions assigned:" + partitions);
				rebalancePartitionsAssignedLatch.countDown();
			}

		});

		container.setConcurrency(2);
		container.setBeanName("testAuto");
		container.start();
		ContainerTestUtils.waitForAssignment(container, embeddedKafka.getPartitionsPerTopic());
		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		ProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<Integer, String>(senderProps);
		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf);
		template.setDefaultTopic(topic1);
		template.sendDefault(0, "foo");
		template.sendDefault(2, "bar");
		template.sendDefault(0, "baz");
		template.sendDefault(2, "qux");
		template.flush();
		assertThat(latch.await(60, TimeUnit.SECONDS)).isTrue();
		assertThat(rebalancePartitionsAssignedLatch.await(60, TimeUnit.SECONDS)).isTrue();
		assertThat(rebalancePartitionsRevokedLatch.await(60, TimeUnit.SECONDS)).isTrue();
		assertThat(listenerThreadNames).allMatch(threadName -> threadName.contains("-consumer-"));
		container.stop();
		this.logger.info("Stop auto");
	}

	@Test
	public void testAfterListenCommit() throws Exception {
		this.logger.info("Start manual");
		Map<String, Object> props = KafkaTestUtils.consumerProps("test2", "false", embeddedKafka);
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<Integer, String>(props);
		ContainerProperties containerProps = new ContainerProperties(topic2);
		ConcurrentMessageListenerContainer<Integer, String> container =
				new ConcurrentMessageListenerContainer<>(cf, containerProps);
		final CountDownLatch latch = new CountDownLatch(4);
		final ArrayList<String> listenerThreadNames = new ArrayList<>();
		containerProps.setMessageListener(new MessageListener<Integer, String>() {

			@Override
			public void onMessage(ConsumerRecord<Integer, String> message) {
				ConcurrentMessageListenerContainerTests.this.logger.info("manual: " + message);
				listenerThreadNames.add(Thread.currentThread().getName());
				latch.countDown();
			}
		});
		container.setConcurrency(2);
		container.setBeanName("testBatch");
		container.start();
		ContainerTestUtils.waitForAssignment(container, embeddedKafka.getPartitionsPerTopic());
		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		ProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<Integer, String>(senderProps);
		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf);
		template.setDefaultTopic(topic2);
		template.sendDefault(0, "foo");
		template.sendDefault(2, "bar");
		template.sendDefault(0, "baz");
		template.sendDefault(2, "qux");
		template.flush();
		assertThat(latch.await(60, TimeUnit.SECONDS)).isTrue();
		assertThat(listenerThreadNames).allMatch(new Predicate<String>() {

			@Override
			public boolean test(String threadName) {
				return threadName.contains("-listener-");
			}
		});
		container.stop();
		this.logger.info("Stop manual");
	}

	@Test
	public void testDefinedPartitions() throws Exception {
		this.logger.info("Start auto parts");
		final Map<String, Object> props = KafkaTestUtils.consumerProps("test3", "true", embeddedKafka);
		TopicPartition topic1Partition0 = new TopicPartition(topic3, 0);

		final CountDownLatch initialConsumersLatch = new CountDownLatch(2);

		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<Integer, String>(props) {

			@Override
			public Consumer<Integer, String> createConsumer() {
				return new KafkaConsumer<Integer, String>(props) {

					@Override
					public ConsumerRecords<Integer, String> poll(long timeout) {
						try {
							return super.poll(timeout);
						}
						finally {
							initialConsumersLatch.countDown();
						}
					}

				};
			}

		};

		ContainerProperties container1Props = new ContainerProperties(topic1Partition0);
		ConcurrentMessageListenerContainer<Integer, String> container1 =
				new ConcurrentMessageListenerContainer<>(cf, container1Props);
		final CountDownLatch latch1 = new CountDownLatch(2);
		container1Props.setMessageListener(new MessageListener<Integer, String>() {

			@Override
			public void onMessage(ConsumerRecord<Integer, String> message) {
				ConcurrentMessageListenerContainerTests.this.logger.info("auto part: " + message);
				latch1.countDown();
			}

		});
		container1.setBeanName("b1");
		container1.start();

		TopicPartition topic1Partition1 = new TopicPartition(topic3, 1);
		ContainerProperties container2Props = new ContainerProperties(topic1Partition1);
		ConcurrentMessageListenerContainer<Integer, String> container2 =
				new ConcurrentMessageListenerContainer<>(cf, container2Props);
		final CountDownLatch latch2 = new CountDownLatch(2);
		container2Props.setMessageListener(new MessageListener<Integer, String>() {

			@Override
			public void onMessage(ConsumerRecord<Integer, String> message) {
				ConcurrentMessageListenerContainerTests.this.logger.info("auto part: " + message);
				latch2.countDown();
			}

		});
		container2.setBeanName("b2");
		container2.start();

		assertThat(initialConsumersLatch.await(20, TimeUnit.SECONDS)).isTrue();

		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		ProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<Integer, String>(senderProps);
		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf);
		template.setDefaultTopic(topic3);
		template.sendDefault(0, 0, "foo");
		template.sendDefault(1, 2, "bar");
		template.sendDefault(0, 0, "baz");
		template.sendDefault(1, 2, "qux");
		template.flush();

		assertThat(latch1.await(60, TimeUnit.SECONDS)).isTrue();
		assertThat(latch2.await(60, TimeUnit.SECONDS)).isTrue();
		container1.stop();
		container2.stop();

		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		cf = new DefaultKafkaConsumerFactory<>(props);
		// reset earliest
		ContainerProperties container3Props = new ContainerProperties(topic1Partition0, topic1Partition1);
		ConcurrentMessageListenerContainer<Integer, String> resettingContainer =
				new ConcurrentMessageListenerContainer<>(cf, container3Props);
		resettingContainer.setBeanName("b3");
		final CountDownLatch latch3 = new CountDownLatch(4);
		container3Props.setMessageListener(new MessageListener<Integer, String>() {

			@Override
			public void onMessage(ConsumerRecord<Integer, String> message) {
				ConcurrentMessageListenerContainerTests.this.logger.info("auto part e: " + message);
				latch3.countDown();
			}
		});
		resettingContainer.start();
		assertThat(latch3.await(60, TimeUnit.SECONDS)).isTrue();
		resettingContainer.stop();
		assertThat(latch3.getCount()).isEqualTo(0L);

		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		cf = new DefaultKafkaConsumerFactory<>(props);
		// reset minusone
		ContainerProperties container4Props = new ContainerProperties(topic1Partition0, topic1Partition1);
		resettingContainer = new ConcurrentMessageListenerContainer<>(cf, container4Props);
		resettingContainer.setBeanName("b4");
		container4Props.setRecentOffset(1);
		final CountDownLatch latch4 = new CountDownLatch(2);
		final AtomicReference<String> receivedMessage = new AtomicReference<>();
		container4Props.setMessageListener(new MessageListener<Integer, String>() {

			@Override
			public void onMessage(ConsumerRecord<Integer, String> message) {
				ConcurrentMessageListenerContainerTests.this.logger.info("auto part -1: " + message);
				receivedMessage.set(message.value());
				latch4.countDown();
			}
		});
		resettingContainer.start();
		assertThat(latch4.await(60, TimeUnit.SECONDS)).isTrue();
		resettingContainer.stop();
		assertThat(receivedMessage.get()).isIn("baz", "qux");
		assertThat(latch4.getCount()).isEqualTo(0L);

		this.logger.info("Stop auto parts");
	}

	@Test
	public void testManualCommit() throws Exception {
		testManualCommitGuts(AckMode.MANUAL, topic4);
		testManualCommitGuts(AckMode.MANUAL_IMMEDIATE_SYNC, topic5);
		// to be sure the commits worked ok so run the tests again and the second tests start at the committed offset.
		testManualCommitGuts(AckMode.MANUAL, topic4);
		testManualCommitGuts(AckMode.MANUAL_IMMEDIATE_SYNC, topic5);
	}

	private void testManualCommitGuts(AckMode ackMode, String topic) throws Exception {
		this.logger.info("Start " + ackMode);
		Map<String, Object> props = KafkaTestUtils.consumerProps("test" + ackMode, "false", embeddedKafka);
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<Integer, String>(props);
		ContainerProperties containerProps = new ContainerProperties(topic);
		ConcurrentMessageListenerContainer<Integer, String> container =
				new ConcurrentMessageListenerContainer<>(cf, containerProps);
		final CountDownLatch latch = new CountDownLatch(4);
		containerProps.setMessageListener(new AcknowledgingMessageListener<Integer, String>() {

			@Override
			public void onMessage(ConsumerRecord<Integer, String> message, Acknowledgment ack) {
				ConcurrentMessageListenerContainerTests.this.logger.info("manual: " + message);
				ack.acknowledge();
				latch.countDown();
			}

		});
		container.setConcurrency(2);
		containerProps.setAckMode(ackMode);
		container.setBeanName("test" + ackMode);
		container.start();
		ContainerTestUtils.waitForAssignment(container, embeddedKafka.getPartitionsPerTopic());
		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		ProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<Integer, String>(senderProps);
		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf);
		template.setDefaultTopic(topic);
		template.sendDefault(0, "foo");
		template.sendDefault(2, "bar");
		template.sendDefault(0, "baz");
		template.sendDefault(2, "qux");
		template.flush();
		assertThat(latch.await(60, TimeUnit.SECONDS)).isTrue();
		container.stop();
		this.logger.info("Stop " + ackMode);
	}

	@Test
	@Ignore // TODO https://github.com/spring-projects/spring-kafka/issues/62 using SYNC for avoidance
	public void testManualCommitExisting() throws Exception {
		this.logger.info("Start MANUAL_IMMEDIATE with Existing");
		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		ProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<Integer, String>(senderProps);
		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf);
		template.setDefaultTopic(topic7);
		template.sendDefault(0, "foo");
		template.sendDefault(2, "bar");
		template.sendDefault(0, "baz");
		template.sendDefault(2, "qux");
		template.flush();
		Map<String, Object> props = KafkaTestUtils.consumerProps("testManualExisting", "false", embeddedKafka);
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<Integer, String>(props);
		ContainerProperties containerProps = new ContainerProperties(topic7);
		ConcurrentMessageListenerContainer<Integer, String> container =
				new ConcurrentMessageListenerContainer<>(cf, containerProps);
		final CountDownLatch latch = new CountDownLatch(8);
		containerProps.setMessageListener(new AcknowledgingMessageListener<Integer, String>() {

			@Override
			public void onMessage(ConsumerRecord<Integer, String> message, Acknowledgment ack) {
				ConcurrentMessageListenerContainerTests.this.logger.info("manualExisting: " + message);
				ack.acknowledge();
				latch.countDown();
			}

		});
		container.setConcurrency(1);
		containerProps.setAckMode(AckMode.MANUAL_IMMEDIATE);
		container.setBeanName("testManualExisting");
		final CountDownLatch commits = new CountDownLatch(8);
		final AtomicReference<Exception> exceptionRef = new AtomicReference<Exception>();
		containerProps.setCommitCallback(new OffsetCommitCallback() {

			@Override
			public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
				commits.countDown();
				if (exception != null) {
					exceptionRef.compareAndSet(null, exception);
				}
			}

		});
		container.start();
		ContainerTestUtils.waitForAssignment(container, embeddedKafka.getPartitionsPerTopic());
		template.sendDefault(0, "fooo");
		template.sendDefault(2, "barr");
		template.sendDefault(0, "bazz");
		template.sendDefault(2, "quxx");
		template.flush();
		assertThat(latch.await(60, TimeUnit.SECONDS)).isTrue();
		assertThat(commits.await(60, TimeUnit.SECONDS)).isTrue();
		assertThat(exceptionRef.get()).isNull();
		container.stop();
		this.logger.info("Stop MANUAL_IMMEDIATE with Existing");
	}

	@Test
	public void testManualCommitSyncExisting() throws Exception {
		this.logger.info("Start MANUAL_IMMEDIATE_SYNC with Existing");
		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		ProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<Integer, String>(senderProps);
		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf);
		template.setDefaultTopic(topic8);
		template.sendDefault(0, "foo");
		template.sendDefault(2, "bar");
		template.sendDefault(0, "baz");
		template.sendDefault(2, "qux");
		template.flush();
		Map<String, Object> props = KafkaTestUtils.consumerProps("testManualExistingSync", "false", embeddedKafka);
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<Integer, String>(props);
		ContainerProperties containerProps = new ContainerProperties(topic8);
		ConcurrentMessageListenerContainer<Integer, String> container =
				new ConcurrentMessageListenerContainer<>(cf, containerProps);
		final CountDownLatch latch = new CountDownLatch(8);
		final BitSet bitSet = new BitSet(8);
		containerProps.setMessageListener(new AcknowledgingMessageListener<Integer, String>() {

			@Override
			public void onMessage(ConsumerRecord<Integer, String> message, Acknowledgment ack) {
				ConcurrentMessageListenerContainerTests.this.logger.info("manualExisting: " + message);
				ack.acknowledge();
				bitSet.set((int) (message.partition() * 4 + message.offset()));
				latch.countDown();
			}

		});
		container.setConcurrency(1);
		containerProps.setAckMode(AckMode.MANUAL_IMMEDIATE_SYNC);
		container.setBeanName("testManualExisting");
		container.start();
		ContainerTestUtils.waitForAssignment(container, embeddedKafka.getPartitionsPerTopic());
		template.sendDefault(0, "fooo");
		template.sendDefault(2, "barr");
		template.sendDefault(0, "bazz");
		template.sendDefault(2, "quxx");
		template.flush();
		assertThat(latch.await(60, TimeUnit.SECONDS)).isTrue();
		assertThat(bitSet.cardinality()).isEqualTo(8);
		container.stop();
		this.logger.info("Stop MANUAL_IMMEDIATE_SYNC with Existing");
	}

	@Test
	@SuppressWarnings("unchecked")
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
		given(cf.createConsumer()).willReturn(consumer);
		given(consumer.poll(anyLong()))
			.willAnswer(new Answer<ConsumerRecords<Integer, String>>() {

				@Override
				public ConsumerRecords<Integer, String> answer(InvocationOnMock invocation) throws Throwable {
					Thread.sleep(100);
					return null;
				}

			});
		ContainerProperties containerProps = new ContainerProperties(topic1PartitionS);
		ConcurrentMessageListenerContainer<Integer, String> container =
				new ConcurrentMessageListenerContainer<>(cf, containerProps);
		containerProps.setMessageListener(new MessageListener<Integer, String>() {

			@Override
			public void onMessage(ConsumerRecord<Integer, String> message) {
			}

		});
		container.setConcurrency(3);
		container.start();
		@SuppressWarnings("unchecked")
		List<KafkaMessageListenerContainer<Integer, String>> containers = KafkaTestUtils.getPropertyValue(container,
				"containers", List.class);
		assertThat(containers.size()).isEqualTo(3);
		for (int i = 0; i < 3; i++) {
			assertThat(KafkaTestUtils.getPropertyValue(containers.get(i), "topicPartitions",
					TopicPartition[].class).length).isEqualTo(i < 2 ? 2 : 3);
		}
		container.stop();
	}

	@Test
	public void testListenerException() throws Exception {
		this.logger.info("Start exception");
		Map<String, Object> props = KafkaTestUtils.consumerProps("test1", "true", embeddedKafka);
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<Integer, String>(props);
		ContainerProperties containerProps = new ContainerProperties(topic6);
		containerProps.setAckCount(23);
		ContainerProperties containerProps2 = new ContainerProperties(topic2);
		BeanUtils.copyProperties(containerProps, containerProps2, "topics", "topicPartitions", "topicPattern");
		ConcurrentMessageListenerContainer<Integer, String> container =
				new ConcurrentMessageListenerContainer<>(cf, containerProps);
		final CountDownLatch latch = new CountDownLatch(4);
		final AtomicBoolean catchError = new AtomicBoolean(false);
		containerProps.setMessageListener(new MessageListener<Integer, String>() {

			@Override
			public void onMessage(ConsumerRecord<Integer, String> message) {
				ConcurrentMessageListenerContainerTests.this.logger.info("auto: " + message);
				latch.countDown();
				throw new RuntimeException("intended");
			}
		});
		container.setConcurrency(2);
		container.setBeanName("testException");
		containerProps.setErrorHandler(new ErrorHandler() {

			@Override
			public void handle(Exception thrownException, ConsumerRecord<?, ?> record) {
				catchError.set(true);
			}

		});
		container.start();
		ContainerTestUtils.waitForAssignment(container, embeddedKafka.getPartitionsPerTopic());
		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		ProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<Integer, String>(senderProps);
		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf);
		template.setDefaultTopic(topic6);
		template.sendDefault(0, "foo");
		template.sendDefault(2, "bar");
		template.sendDefault(0, "baz");
		template.sendDefault(2, "qux");
		template.flush();
		assertThat(latch.await(60, TimeUnit.SECONDS)).isTrue();
		assertThat(catchError.get()).isTrue();
		container.stop();
		this.logger.info("Stop exception");

	}

}
