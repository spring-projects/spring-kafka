/*
 * Copyright 2016-2018 the original author or authors.
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

package org.springframework.kafka.listener;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
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
import org.springframework.kafka.support.TopicPartitionInitialOffset;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;

/**
 * @author Gary Russell
 * @author Artem Bilan
 * @author Jerome Mirc
 * @author Marius Bogoevici
 * @author Artem Yakshin
 */
public class ConcurrentMessageListenerContainerTests {

	private final Log logger = LogFactory.getLog(this.getClass());

	private static String topic1 = "testTopic1";

	private static String topic2 = "testTopic2";

//	private static String topic3 = "testTopic3";

	private static String topic4 = "testTopic4";

	private static String topic5 = "testTopic5";

	private static String topic6 = "testTopic6";

	private static String topic7 = "testTopic7";

	private static String topic8 = "testTopic8";

	private static String topic9 = "testTopic9";

	private static String topic10 = "testTopic10";

	private static String topic11 = "testTopic11";

	@ClassRule
	public static KafkaEmbedded embeddedKafka = new KafkaEmbedded(1, true, topic1, topic2, topic4, topic5,
			topic6, topic7, topic8, topic9, topic10, topic11);

	@Test
	public void testAutoCommit() throws Exception {
		this.logger.info("Start auto");
		Map<String, Object> props = KafkaTestUtils.consumerProps("test1", "true", embeddedKafka);
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(props);
		ContainerProperties containerProps = new ContainerProperties(topic1);

		final CountDownLatch latch = new CountDownLatch(4);
		final Set<String> listenerThreadNames = new ConcurrentSkipListSet<>();
		containerProps.setMessageListener((MessageListener<Integer, String>) message -> {
			ConcurrentMessageListenerContainerTests.this.logger.info("auto: " + message);
			listenerThreadNames.add(Thread.currentThread().getName());
			latch.countDown();
		});

		ConcurrentMessageListenerContainer<Integer, String> container =
				new ConcurrentMessageListenerContainer<>(cf, containerProps);
		container.setConcurrency(2);
		container.setBeanName("testAuto");
		container.start();

		ContainerTestUtils.waitForAssignment(container, embeddedKafka.getPartitionsPerTopic());

		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		ProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf);
		template.setDefaultTopic(topic1);
		template.sendDefault(0, "foo");
		template.sendDefault(2, "bar");
		template.sendDefault(0, "baz");
		template.sendDefault(2, "qux");
		template.flush();
		assertThat(latch.await(60, TimeUnit.SECONDS)).isTrue();
		for (String threadName : listenerThreadNames) {
			assertThat(threadName).contains("-C-");
		}
		@SuppressWarnings("unchecked")
		List<KafkaMessageListenerContainer<Integer, String>> containers = KafkaTestUtils.getPropertyValue(container,
				"containers", List.class);
		assertThat(containers.size()).isEqualTo(2);
		for (int i = 0; i < 2; i++) {
			assertThat(KafkaTestUtils.getPropertyValue(containers.get(i), "listenerConsumer.acks", Collection.class)
					.size()).isEqualTo(0);
		}
		container.stop();
		this.logger.info("Stop auto");
	}

	@Test
	public void testAutoCommitWithRebalanceListener() throws Exception {
		this.logger.info("Start auto");
		Map<String, Object> props = KafkaTestUtils.consumerProps("test10", "true", embeddedKafka);
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(props);
		ContainerProperties containerProps = new ContainerProperties(topic1);

		final CountDownLatch latch = new CountDownLatch(4);
		final Set<String> listenerThreadNames = new ConcurrentSkipListSet<>();
		containerProps.setMessageListener((MessageListener<Integer, String>) message -> {
			ConcurrentMessageListenerContainerTests.this.logger.info("auto: " + message);
			listenerThreadNames.add(Thread.currentThread().getName());
			latch.countDown();
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

		ConcurrentMessageListenerContainer<Integer, String> container =
				new ConcurrentMessageListenerContainer<>(cf, containerProps);
		container.setConcurrency(2);
		container.setBeanName("testAuto");
		container.start();

		ContainerTestUtils.waitForAssignment(container, embeddedKafka.getPartitionsPerTopic());

		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		ProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
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
		for (String threadName : listenerThreadNames) {
			assertThat(threadName).contains("-C-");
		}
		container.stop();
		this.logger.info("Stop auto");
	}

	@Test
	public void testAfterListenCommit() throws Exception {
		this.logger.info("Start manual");
		Map<String, Object> props = KafkaTestUtils.consumerProps("test2", "false", embeddedKafka);
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(props);
		ContainerProperties containerProps = new ContainerProperties(topic2);

		final CountDownLatch latch = new CountDownLatch(4);
		final Set<String> listenerThreadNames = new ConcurrentSkipListSet<>();
		containerProps.setMessageListener((MessageListener<Integer, String>) message -> {
			ConcurrentMessageListenerContainerTests.this.logger.info("manual: " + message);
			listenerThreadNames.add(Thread.currentThread().getName());
			latch.countDown();
		});

		ConcurrentMessageListenerContainer<Integer, String> container =
				new ConcurrentMessageListenerContainer<>(cf, containerProps);
		container.setConcurrency(2);
		container.setBeanName("testBatch");
		container.start();

		ContainerTestUtils.waitForAssignment(container, embeddedKafka.getPartitionsPerTopic());

		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		ProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf);
		template.setDefaultTopic(topic2);
		template.sendDefault(0, "foo");
		template.sendDefault(2, "bar");
		template.sendDefault(0, "baz");
		template.sendDefault(2, "qux");
		template.flush();
		assertThat(latch.await(60, TimeUnit.SECONDS)).isTrue();
		for (String threadName : listenerThreadNames) {
			assertThat(threadName).contains("-L-");
		}
		container.stop();
		this.logger.info("Stop manual");
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
		this.logger.info("Start " + ackMode);
		Map<String, Object> props = KafkaTestUtils.consumerProps("test" + ackMode, "false", embeddedKafka);
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(props);
		ContainerProperties containerProps = new ContainerProperties(topic);
		final CountDownLatch latch = new CountDownLatch(4);
		containerProps.setMessageListener((AcknowledgingMessageListener<Integer, String>) (message, ack) -> {
			ConcurrentMessageListenerContainerTests.this.logger.info("manual: " + message);
			ack.acknowledge();
			latch.countDown();
		});

		containerProps.setAckMode(ackMode);
		ConcurrentMessageListenerContainer<Integer, String> container =
				new ConcurrentMessageListenerContainer<>(cf, containerProps);
		container.setConcurrency(2);
		container.setBeanName("test" + ackMode);
		container.start();

		ContainerTestUtils.waitForAssignment(container, embeddedKafka.getPartitionsPerTopic());

		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		ProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
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
		ProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf);
		template.setDefaultTopic(topic7);
		template.sendDefault(0, "foo");
		template.sendDefault(2, "bar");
		template.sendDefault(0, "baz");
		template.sendDefault(2, "qux");
		template.flush();
		Map<String, Object> props = KafkaTestUtils.consumerProps("testManualExisting", "false", embeddedKafka);
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(props);
		ContainerProperties containerProps = new ContainerProperties(topic7);
		final CountDownLatch latch = new CountDownLatch(8);
		containerProps.setMessageListener((AcknowledgingMessageListener<Integer, String>) (message, ack) -> {
			ConcurrentMessageListenerContainerTests.this.logger.info("manualExisting: " + message);
			ack.acknowledge();
			latch.countDown();
		});
		containerProps.setAckMode(AckMode.MANUAL_IMMEDIATE);

		final CountDownLatch commits = new CountDownLatch(8);
		final AtomicReference<Exception> exceptionRef = new AtomicReference<>();
		containerProps.setCommitCallback((offsets, exception) -> {
			commits.countDown();
			if (exception != null) {
				exceptionRef.compareAndSet(null, exception);
			}
		});

		ConcurrentMessageListenerContainer<Integer, String> container =
				new ConcurrentMessageListenerContainer<>(cf, containerProps);
		container.setConcurrency(1);
		container.setBeanName("testManualExisting");

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
		this.logger.info("Start MANUAL_IMMEDIATE with Existing");
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
		containerProps.setSyncCommits(true);
		final CountDownLatch latch = new CountDownLatch(8);
		final BitSet bitSet = new BitSet(8);
		containerProps.setMessageListener((AcknowledgingMessageListener<Integer, String>) (message, ack) -> {
			ConcurrentMessageListenerContainerTests.this.logger.info("manualExisting: " + message);
			ack.acknowledge();
			bitSet.set((int) (message.partition() * 4 + message.offset()));
			latch.countDown();
		});
		containerProps.setAckMode(AckMode.MANUAL_IMMEDIATE);

		ConcurrentMessageListenerContainer<Integer, String> container =
				new ConcurrentMessageListenerContainer<>(cf, containerProps);
		container.setConcurrency(1);
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
		this.logger.info("Stop MANUAL_IMMEDIATE with Existing");
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testConcurrencyWithPartitions() {
		TopicPartitionInitialOffset[] topic1PartitionS = new TopicPartitionInitialOffset[] {
				new TopicPartitionInitialOffset(topic1, 0),
				new TopicPartitionInitialOffset(topic1, 1),
				new TopicPartitionInitialOffset(topic1, 2),
				new TopicPartitionInitialOffset(topic1, 3),
				new TopicPartitionInitialOffset(topic1, 4),
				new TopicPartitionInitialOffset(topic1, 5),
				new TopicPartitionInitialOffset(topic1, 6)
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
		containerProps.setMessageListener((MessageListener<Integer, String>) message -> { });

		ConcurrentMessageListenerContainer<Integer, String> container =
				new ConcurrentMessageListenerContainer<>(cf, containerProps);
		container.setConcurrency(3);
		container.start();
		List<KafkaMessageListenerContainer<Integer, String>> containers = KafkaTestUtils.getPropertyValue(container,
				"containers", List.class);
		assertThat(containers.size()).isEqualTo(3);
		for (int i = 0; i < 3; i++) {
			assertThat(KafkaTestUtils.getPropertyValue(containers.get(i), "topicPartitions",
					TopicPartitionInitialOffset[].class).length).isEqualTo(i < 2 ? 2 : 3);
		}
		container.stop();
	}

	@Test
	public void testListenerException() throws Exception {
		this.logger.info("Start exception");
		Map<String, Object> props = KafkaTestUtils.consumerProps("test1", "true", embeddedKafka);
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(props);
		ContainerProperties containerProps = new ContainerProperties(topic6);
		containerProps.setAckCount(23);
		ContainerProperties containerProps2 = new ContainerProperties(topic2);
		BeanUtils.copyProperties(containerProps, containerProps2,
				"topics", "topicPartitions", "topicPattern", "ackCount", "ackTime");
		final CountDownLatch latch = new CountDownLatch(4);
		final AtomicBoolean catchError = new AtomicBoolean(false);
		containerProps.setMessageListener((MessageListener<Integer, String>) message -> {
			ConcurrentMessageListenerContainerTests.this.logger.info("auto: " + message);
			latch.countDown();
			throw new RuntimeException("intended");
		});
		containerProps.setGenericErrorHandler((ErrorHandler) (thrownException, record) -> catchError.set(true));

		ConcurrentMessageListenerContainer<Integer, String> container =
				new ConcurrentMessageListenerContainer<>(cf, containerProps);
		container.setConcurrency(2);
		container.setBeanName("testException");

		container.start();
		ContainerTestUtils.waitForAssignment(container, embeddedKafka.getPartitionsPerTopic());
		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		ProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
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


	@Test
	public void testAckOnErrorRecord() throws Exception {
		logger.info("Start ack on error");
		Map<String, Object> props = KafkaTestUtils.consumerProps("test9", "false", embeddedKafka);
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(props);
		final CountDownLatch latch = new CountDownLatch(4);
		ContainerProperties containerProps = new ContainerProperties(topic9);
		containerProps.setMessageListener((MessageListener<Integer, String>) message -> {
			logger.info("auto ack on error: " + message);
			latch.countDown();
			if (message.value().startsWith("b")) {
				throw new RuntimeException();
			}
		});
		containerProps.setSyncCommits(true);
		containerProps.setAckMode(AckMode.RECORD);
		containerProps.setAckOnError(false);
		ConcurrentMessageListenerContainer<Integer, String> container = new ConcurrentMessageListenerContainer<>(cf,
				containerProps);
		container.setConcurrency(2);
		container.setBeanName("testAckOnError");
		container.start();
		ContainerTestUtils.waitForAssignment(container, embeddedKafka.getPartitionsPerTopic());
		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		ProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf);
		template.setDefaultTopic(topic9);
		template.sendDefault(0, 0, "foo");
		template.sendDefault(1, 0, "bar");
		template.sendDefault(0, 0, "baz");
		template.sendDefault(1, 0, "qux");
		template.flush();
		assertThat(latch.await(60, TimeUnit.SECONDS)).isTrue();
		container.stop();
		Consumer<Integer, String> consumer = cf.createConsumer();
		consumer.assign(Arrays.asList(new TopicPartition(topic9, 0), new TopicPartition(topic9, 1)));
		// this consumer is positioned at 1, the next offset after the successfully
		// processed 'foo'
		// it has not been updated because 'bar' failed
		// Since there is no simple ability to hook into 'commitSync()' action with concurrent containers,
		// ping partition until success through some sleep period
		for (int i = 0; i < 100; i++) {
			if (consumer.position(new TopicPartition(topic9, 0)) == 1) {
				break;
			}
			else {
				Thread.sleep(100);
			}
		}
		assertThat(consumer.position(new TopicPartition(topic9, 0))).isEqualTo(1);
		// this consumer is positioned at 1, the next offset after the successfully
		// processed 'qux'
		// it has been updated even 'baz' failed
		for (int i = 0; i < 100; i++) {
			if (consumer.position(new TopicPartition(topic9, 1)) == 2) {
				break;
			}
			else {
				Thread.sleep(100);
			}
		}
		assertThat(consumer.position(new TopicPartition(topic9, 1))).isEqualTo(2);
		consumer.close();
		logger.info("Stop ack on error");
	}

	@Test
	public void testAckOnErrorManualImmediate() throws  Exception {
		//ackOnError should not affect manual commits
		testAckOnErrorWithManualImmediateGuts(topic10, true);
		testAckOnErrorWithManualImmediateGuts(topic11, false);
	}

	private void testAckOnErrorWithManualImmediateGuts(String topic, boolean ackOnError) throws Exception {
		logger.info("Start ack on error with ManualImmediate ack mode");
		Map<String, Object> props = KafkaTestUtils.consumerProps("testMan" + ackOnError, "false", embeddedKafka);
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<Integer, String>(props);
		final CountDownLatch latch = new CountDownLatch(2);
		ContainerProperties containerProps = new ContainerProperties(topic);
		containerProps.setSyncCommits(true);
		containerProps.setAckMode(AckMode.MANUAL_IMMEDIATE);
		containerProps.setAckOnError(ackOnError);
		containerProps.setMessageListener((AcknowledgingMessageListener<Integer, String>) (message, ack) -> {
			ConcurrentMessageListenerContainerTests.this.logger.info("manualExisting: " + message);
			latch.countDown();
			if (message.value().startsWith("b")) {
				throw new RuntimeException();
			}
			else {
				ack.acknowledge();
			}

		});
		ConcurrentMessageListenerContainer<Integer, String> container = new ConcurrentMessageListenerContainer<>(cf,
				containerProps);
		container.setConcurrency(1);
		container.setBeanName("testAckOnErrorWithManualImmediate");
		container.start();
		ContainerTestUtils.waitForAssignment(container, embeddedKafka.getPartitionsPerTopic());

		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		ProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf);
		template.setDefaultTopic(topic);
		template.sendDefault(0, 0, "foo");
		template.sendDefault(0, 1, "bar");
		template.flush();
		assertThat(latch.await(60, TimeUnit.SECONDS)).isTrue();
		container.stop();

		Consumer<Integer, String> consumer = cf.createConsumer();
		consumer.assign(Arrays.asList(new TopicPartition(topic, 0), new TopicPartition(topic, 0)));

		// only one message should be acknowledged, because second, starting with "b"
		// will throw RunTimeException and acknowledge() method will not invoke on it
		for (int i = 0; i < 300; i++) {
			if (consumer.position(new TopicPartition(topic, 0)) == 1) {
				break;
			}
			else {
				Thread.sleep(100);
			}
		}
		assertThat(consumer.position(new TopicPartition(topic, 0))).isEqualTo(1);
		consumer.close();
		logger.info("Stop ack on error with ManualImmediate ack mode");
	}

	@Test
	public void testRebalanceWithSlowConsumer() throws Exception {
		this.logger.info("Start auto");
		Map<String, Object> props = KafkaTestUtils.consumerProps("test101", "false", embeddedKafka);
		props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "20000");
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(props);
		ContainerProperties containerProps = new ContainerProperties(topic1);
		final CountDownLatch latch = new CountDownLatch(8);
		final Set<String> listenerThreadNames = Collections.synchronizedSet(new HashSet<String>());
		List<String> receivedMessages = Collections.synchronizedList(new ArrayList<>());
		containerProps.setMessageListener((MessageListener<Integer, String>) message -> {
			listenerThreadNames.add(Thread.currentThread().getName());
			try {
				Thread.sleep(2000);
			}
			catch (InterruptedException e) {
				// ignore
			}
			receivedMessages.add(message.value());
			listenerThreadNames.add(Thread.currentThread().getName());
			latch.countDown();
		});

		ConcurrentMessageListenerContainer<Integer, String> container =
				new ConcurrentMessageListenerContainer<>(cf, containerProps);
		ConcurrentMessageListenerContainer<Integer, String> container2 =
				new ConcurrentMessageListenerContainer<>(cf, containerProps);
		container.setConcurrency(1);
		container2.setConcurrency(1);
		container.setBeanName("testAuto");
		container2.setBeanName("testAuto2");
		container.start();
		ContainerTestUtils.waitForAssignment(container, embeddedKafka.getPartitionsPerTopic());
		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		ProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf);
		template.setDefaultTopic(topic1);
		template.sendDefault(0, 0, "foo");
		template.sendDefault(0, 2, "bar");
		template.sendDefault(0, 0, "baz");
		template.sendDefault(0, 2, "qux");
		template.sendDefault(1, 2, "corge");
		template.sendDefault(1, 2, "grault");
		template.sendDefault(1, 2, "garply");
		template.sendDefault(1, 2, "waldo");
		template.flush();
		container2.start();
		assertThat(latch.await(60, TimeUnit.SECONDS)).isTrue();
		assertThat(receivedMessages).containsOnlyOnce("foo", "bar", "baz", "qux", "corge", "grault", "garply", "waldo");
		// all messages are received
		assertThat(receivedMessages).hasSize(8);
		// messages are received on separate threads
		assertThat(listenerThreadNames.size()).isGreaterThanOrEqualTo(2);
		container.stop();
		container2.stop();
		this.logger.info("Stop auto");
	}

}
