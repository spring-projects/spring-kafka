/*
 * Copyright 2016-2019 the original author or authors.
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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.springframework.core.log.LogAccessor;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.event.ContainerStoppedEvent;
import org.springframework.kafka.event.KafkaEvent;
import org.springframework.kafka.support.TopicPartitionOffset;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.condition.EmbeddedKafkaCondition;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;

/**
 * @author Gary Russell
 * @author Artem Bilan
 * @author Jerome Mirc
 * @author Marius Bogoevici
 * @author Artem Yakshin
 * @author Vladimir Tsanev
 */
@EmbeddedKafka(topics = { ConcurrentMessageListenerContainerTests.topic1,
		ConcurrentMessageListenerContainerTests.topic2,
		ConcurrentMessageListenerContainerTests.topic4, ConcurrentMessageListenerContainerTests.topic5,
		ConcurrentMessageListenerContainerTests.topic6, ConcurrentMessageListenerContainerTests.topic7,
		ConcurrentMessageListenerContainerTests.topic8, ConcurrentMessageListenerContainerTests.topic9,
		ConcurrentMessageListenerContainerTests.topic10, ConcurrentMessageListenerContainerTests.topic11,
		ConcurrentMessageListenerContainerTests.topic12 })
public class ConcurrentMessageListenerContainerTests {

	private final LogAccessor logger = new LogAccessor(LogFactory.getLog(this.getClass()));

	public static final String topic1 = "testTopic1";

	public static final String topic2 = "testTopic2";

	public static final String topic4 = "testTopic4";

	public static final String topic5 = "testTopic5";

	public static final String topic6 = "testTopic6";

	public static final String topic7 = "testTopic7";

	public static final String topic8 = "testTopic8";

	public static final String topic9 = "testTopic9";

	public static final String topic10 = "testTopic10";

	public static final String topic11 = "testTopic11";

	public static final String topic12 = "testTopic12";

	private static EmbeddedKafkaBroker embeddedKafka;

	@BeforeAll
	public static void setup() {
		embeddedKafka = EmbeddedKafkaCondition.getBroker();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testAutoCommit() throws Exception {
		this.logger.info("Start auto");
		Map<String, Object> props = KafkaTestUtils.consumerProps("test1", "true", embeddedKafka);
		AtomicReference<Properties> overrides = new AtomicReference<>();
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<Integer, String>(props) {

			@Override
			protected KafkaConsumer<Integer, String> createKafkaConsumer(String groupId, String clientIdPrefix,
					String clientIdSuffixArg, Properties properties) {

				overrides.set(properties);
				return super.createKafkaConsumer(groupId, clientIdPrefix, clientIdSuffixArg, properties);
			}

		};
		ContainerProperties containerProps = new ContainerProperties(topic1);
		containerProps.setLogContainerConfig(true);

		final CountDownLatch latch = new CountDownLatch(3);
		final Set<String> listenerThreadNames = new ConcurrentSkipListSet<>();
		final List<String> payloads = new ArrayList<>();
		containerProps.setMessageListener((MessageListener<Integer, String>) message -> {
			ConcurrentMessageListenerContainerTests.this.logger.info("auto: " + message);
			listenerThreadNames.add(Thread.currentThread().getName());
			payloads.add(message.value());
			latch.countDown();
		});

		ConcurrentMessageListenerContainer<Integer, String> container =
				new ConcurrentMessageListenerContainer<>(cf, containerProps);
		container.setConcurrency(2);
		container.setBeanName("testAuto");
		List<KafkaEvent> events = new ArrayList<>();
		CountDownLatch stopLatch = new CountDownLatch(3);
		container.setApplicationEventPublisher(e -> {
			events.add((KafkaEvent) e);
			if (e instanceof ContainerStoppedEvent) {
				stopLatch.countDown();
			}
		});
		CountDownLatch intercepted = new CountDownLatch(4);
		container.setRecordInterceptor(record -> {
			intercepted.countDown();
			return record.value().equals("baz") ? null : record;
		});
		container.start();

		ContainerTestUtils.waitForAssignment(container, embeddedKafka.getPartitionsPerTopic());
		assertThat(container.getAssignedPartitions()).hasSize(2);

		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		ProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf);
		template.setDefaultTopic(topic1);
		template.sendDefault(0, "foo");
		template.sendDefault(2, "bar");
		template.sendDefault(0, "baz");
		template.sendDefault(2, "qux");
		template.flush();
		assertThat(intercepted.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(latch.await(60, TimeUnit.SECONDS)).isTrue();
		for (String threadName : listenerThreadNames) {
			assertThat(threadName).contains("-C-");
		}
		List<KafkaMessageListenerContainer<Integer, String>> containers = KafkaTestUtils.getPropertyValue(container,
				"containers", List.class);
		assertThat(containers).hasSize(2);
		for (int i = 0; i < 2; i++) {
			assertThat(KafkaTestUtils.getPropertyValue(containers.get(i), "listenerConsumer.acks", Collection.class)
					.size()).isEqualTo(0);
		}
		assertThat(container.metrics()).isNotNull();
		Set<KafkaMessageListenerContainer<Integer, String>> children = new HashSet<>(containers);
		container.stop();
		assertThat(stopLatch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(payloads).containsExactlyInAnyOrder("foo", "bar", "qux");
		events.forEach(e -> {
			assertThat(e.getContainer(MessageListenerContainer.class)).isSameAs(container);
			if (e instanceof ContainerStoppedEvent) {
				if (e.getSource().equals(container)) {
					assertThat(e.getContainer(MessageListenerContainer.class)).isSameAs(container);
				}
				else {
					assertThat(children).contains((KafkaMessageListenerContainer<Integer, String>) e.getSource());
				}
			}
			else {
				assertThat(children).contains((KafkaMessageListenerContainer<Integer, String>) e.getSource());
			}
		});
		assertThat(overrides.get().getProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG)).isNull();
		this.logger.info("Stop auto");
	}

	@Test
	public void testAutoCommitWithRebalanceListener() throws Exception {
		this.logger.info("Start auto");
		Map<String, Object> props = KafkaTestUtils.consumerProps("test10", "false", embeddedKafka);
		AtomicReference<Properties> overrides = new AtomicReference<>();
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<Integer, String>(props) {

			@Override
			protected KafkaConsumer<Integer, String> createKafkaConsumer(String groupId, String clientIdPrefix,
					String clientIdSuffixArg, Properties properties) {

				overrides.set(properties);
				return super.createKafkaConsumer(groupId, clientIdPrefix, clientIdSuffixArg, properties);
			}

		};
		ContainerProperties containerProps = new ContainerProperties(topic1);

		final CountDownLatch latch = new CountDownLatch(4);
		final Set<String> listenerThreadNames = new ConcurrentSkipListSet<>();
		containerProps.setMessageListener((MessageListener<Integer, String>) message -> {
			ConcurrentMessageListenerContainerTests.this.logger.info("auto: " + message);
			listenerThreadNames.add(Thread.currentThread().getName());
			latch.countDown();
		});
		Properties consumerProperties = new Properties();
		consumerProperties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
		containerProps.setKafkaConsumerProperties(consumerProperties);
		final CountDownLatch rebalancePartitionsAssignedLatch = new CountDownLatch(2);
		containerProps.setConsumerRebalanceListener(new ConsumerRebalanceListener() {

			@Override
			public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
				ConcurrentMessageListenerContainerTests.this.logger.info("In test, partitions revoked:" + partitions);
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
		for (String threadName : listenerThreadNames) {
			assertThat(threadName).contains("-C-");
		}
		container.stop();
		assertThat(overrides.get().getProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG)).isEqualTo("true");
		this.logger.info("Stop auto");
	}

	@Test
	public void testAfterListenCommit() throws Exception {
		this.logger.info("Start manual");
		Map<String, Object> props = KafkaTestUtils.consumerProps("test2", "false", embeddedKafka);
		props.remove(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG);
		AtomicReference<Properties> overrides = new AtomicReference<>();
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<Integer, String>(props) {

			@Override
			protected KafkaConsumer<Integer, String> createKafkaConsumer(String groupId, String clientIdPrefix,
					String clientIdSuffixArg, Properties properties) {

				overrides.set(properties);
				return super.createKafkaConsumer(groupId, clientIdPrefix, clientIdSuffixArg, properties);
			}

		};
		ContainerProperties containerProps = new ContainerProperties(topic2);

		final CountDownLatch latch = new CountDownLatch(4);
		containerProps.setMessageListener((MessageListener<Integer, String>) message -> {
			ConcurrentMessageListenerContainerTests.this.logger.info("manual: " + message);
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
		container.stop();
		this.logger.info("Stop manual");
		assertThat(overrides.get().getProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG)).isEqualTo("false");
	}

	@Test
	public void testManualCommit() throws Exception {
		testManualCommitGuts(ContainerProperties.AckMode.MANUAL, topic4, 1);
		testManualCommitGuts(ContainerProperties.AckMode.MANUAL_IMMEDIATE, topic5, 1);
		// to be sure the commits worked ok so run the tests again and the second tests start at the committed offset.
		testManualCommitGuts(ContainerProperties.AckMode.MANUAL, topic4, 2);
		testManualCommitGuts(ContainerProperties.AckMode.MANUAL_IMMEDIATE, topic5, 2);
	}

	private void testManualCommitGuts(ContainerProperties.AckMode ackMode, String topic, int qual) throws Exception {
		this.logger.info("Start " + ackMode);
		Map<String, Object> props = KafkaTestUtils.consumerProps("test" + ackMode + qual, "false", embeddedKafka);
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
		containerProps.setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
		containerProps.setSyncCommits(false);
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
		containerProps.setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);

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
	public void testPausedStart() throws Exception {
		this.logger.info("Start paused start");
		Map<String, Object> props = KafkaTestUtils.consumerProps("test12", "false", embeddedKafka);
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(props);
		ContainerProperties containerProps = new ContainerProperties(topic12);

		final CountDownLatch latch = new CountDownLatch(2);
		containerProps.setMessageListener((MessageListener<Integer, String>) message -> {
			ConcurrentMessageListenerContainerTests.this.logger.info("paused start: " + message);
			latch.countDown();
		});

		ConcurrentMessageListenerContainer<Integer, String> container =
				new ConcurrentMessageListenerContainer<>(cf, containerProps);
		container.setConcurrency(2);
		container.setBeanName("testBatch");
		container.pause();
		container.start();

		ContainerTestUtils.waitForAssignment(container, embeddedKafka.getPartitionsPerTopic());

		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		ProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf);
		template.setDefaultTopic(topic12);
		template.sendDefault(0, "foo");
		template.sendDefault(2, "bar");
		template.flush();
		assertThat(latch.await(100, TimeUnit.MILLISECONDS)).isFalse();

		container.resume();

		assertThat(latch.await(60, TimeUnit.SECONDS)).isTrue();
		container.stop();
		this.logger.info("Stop paused start");
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testConcurrencyWithPartitions() {
		TopicPartitionOffset[] topic1PartitionS = new TopicPartitionOffset[] {
				new TopicPartitionOffset(topic1, 0),
				new TopicPartitionOffset(topic1, 1),
				new TopicPartitionOffset(topic1, 2),
				new TopicPartitionOffset(topic1, 3),
				new TopicPartitionOffset(topic1, 4),
				new TopicPartitionOffset(topic1, 5),
				new TopicPartitionOffset(topic1, 6)
		};
		ConsumerFactory<Integer, String> cf = mock(ConsumerFactory.class);
		Consumer<Integer, String> consumer = mock(Consumer.class);
		given(cf.createConsumer(anyString(), anyString(), anyString(), any())).willReturn(consumer);
		given(consumer.poll(any(Duration.class)))
			.willAnswer(new Answer<ConsumerRecords<Integer, String>>() {

				@Override
				public ConsumerRecords<Integer, String> answer(InvocationOnMock invocation) throws Throwable {
					Thread.sleep(100);
					return null;
				}

			});
		ContainerProperties containerProps = new ContainerProperties(topic1PartitionS);
		containerProps.setGroupId("grp");
		containerProps.setMessageListener((MessageListener<Integer, String>) message -> { });
		containerProps.setMissingTopicsFatal(false);

		ConcurrentMessageListenerContainer<Integer, String> container =
				new ConcurrentMessageListenerContainer<>(cf, containerProps);
		container.setConcurrency(3);
		container.start();
		List<KafkaMessageListenerContainer<Integer, String>> containers = KafkaTestUtils.getPropertyValue(container,
				"containers", List.class);
		assertThat(containers).hasSize(3);
		for (int i = 0; i < 3; i++) {
			assertThat(KafkaTestUtils.getPropertyValue(containers.get(i), "topicPartitions",
					TopicPartitionOffset[].class).length).isEqualTo(i < 2 ? 2 : 3);
		}
		container.stop();
	}

	@Test
	public void testListenerException() throws Exception {
		this.logger.info("Start exception");
		Map<String, Object> props = KafkaTestUtils.consumerProps("test1", "true", embeddedKafka);
		props.remove(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG);
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(props);
		ContainerProperties containerProps = new ContainerProperties(topic6);
		containerProps.setAckCount(23);
		containerProps.setGroupId("testListenerException");
		final CountDownLatch latch = new CountDownLatch(4);
		final AtomicBoolean catchError = new AtomicBoolean(false);
		containerProps.setMessageListener((MessageListener<Integer, String>) message -> {
			ConcurrentMessageListenerContainerTests.this.logger.info("auto: " + message);
			latch.countDown();
			throw new RuntimeException("intended");
		});
		Properties consumerProperties = new Properties();
		consumerProperties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
		containerProps.setKafkaConsumerProperties(consumerProperties);

		ConcurrentMessageListenerContainer<Integer, String> container =
				new ConcurrentMessageListenerContainer<>(cf, containerProps);
		container.setConcurrency(2);
		container.setBeanName("testException");
		container.setErrorHandler((thrownException, record) -> catchError.set(true));

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
		containerProps.setAckMode(ContainerProperties.AckMode.RECORD);
		containerProps.setAckOnError(false);
		ConcurrentMessageListenerContainer<Integer, String> container = new ConcurrentMessageListenerContainer<>(cf,
				containerProps);
		container.setConcurrency(2);
		container.setBeanName("testAckOnError");
		container.setErrorHandler(new LoggingErrorHandler() {

			@Override
			public void handle(Exception thrownException, ConsumerRecord<?, ?> record) {
				// nothing
			}

			@Override
			public boolean isAckAfterHandle() {
				return false;
			}

		});
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
		// this consumer is positioned at 2, the next offset after the successfully
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
		containerProps.setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
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
		// will throw RuntimeException and acknowledge() method will not invoke on it
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

}
