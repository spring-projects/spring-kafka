/*
 * Copyright 2016-present the original author or authors.
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

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.jspecify.annotations.Nullable;
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
import org.springframework.kafka.event.ConcurrentContainerStoppedEvent;
import org.springframework.kafka.event.ConsumerStoppedEvent;
import org.springframework.kafka.event.ContainerStoppedEvent;
import org.springframework.kafka.event.KafkaEvent;
import org.springframework.kafka.support.TopicPartitionOffset;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.condition.EmbeddedKafkaCondition;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

/**
 * @author Gary Russell
 * @author Artem Bilan
 * @author Jerome Mirc
 * @author Marius Bogoevici
 * @author Artem Yakshin
 * @author Vladimir Tsanev
 * @author Soby Chacko
 * @author Lokesh Alamuri
 */
@EmbeddedKafka(topics = {ConcurrentMessageListenerContainerTests.topic1,
		ConcurrentMessageListenerContainerTests.topic2,
		ConcurrentMessageListenerContainerTests.topic4, ConcurrentMessageListenerContainerTests.topic5,
		ConcurrentMessageListenerContainerTests.topic6, ConcurrentMessageListenerContainerTests.topic7,
		ConcurrentMessageListenerContainerTests.topic8, ConcurrentMessageListenerContainerTests.topic9,
		ConcurrentMessageListenerContainerTests.topic10, ConcurrentMessageListenerContainerTests.topic11,
		ConcurrentMessageListenerContainerTests.topic12, ConcurrentMessageListenerContainerTests.topic13},
		brokerProperties = "group.initial.rebalance.delay.ms:500")
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

	public static final String topic13 = "testTopic13";

	private static EmbeddedKafkaBroker embeddedKafka;

	@BeforeAll
	public static void setup() {
		embeddedKafka = EmbeddedKafkaCondition.getBroker();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testAutoCommit() throws Exception {
		this.logger.info("Start auto");
		Map<String, Object> props = KafkaTestUtils.consumerProps(embeddedKafka, "test1", true);
		AtomicReference<Properties> overrides = new AtomicReference<>();
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<Integer, String>(props) {

			@Override
			protected Consumer<Integer, String> createKafkaConsumer(String groupId, String clientIdPrefix,
					String clientIdSuffixArg, Properties properties) {

				overrides.set(properties);
				return super.createKafkaConsumer(groupId, clientIdPrefix, clientIdSuffixArg, properties);
			}

		};
		ContainerProperties containerProps = new ContainerProperties(topic1);
		containerProps.setLogContainerConfig(true);
		containerProps.setClientId("client");

		final CountDownLatch latch = new CountDownLatch(3);
		final Set<String> listenerThreadNames = new ConcurrentSkipListSet<>();
		List<String> payloads = Collections.synchronizedList(new ArrayList<>());
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
		container.setChangeConsumerThreadName(true);
		BlockingQueue<KafkaEvent> events = new LinkedBlockingQueue<>();
		CountDownLatch stopLatch = new CountDownLatch(4);
		CountDownLatch concurrentContainerStopLatch = new CountDownLatch(1);
		container.setApplicationEventPublisher(e -> {
			events.add((KafkaEvent) e);
			if (e instanceof ContainerStoppedEvent) {
				stopLatch.countDown();
			}
			if (e instanceof ConcurrentContainerStoppedEvent) {
				concurrentContainerStopLatch.countDown();
			}
		});
		CountDownLatch intercepted = new CountDownLatch(4);
		container.setRecordInterceptor((record, consumer) -> {
			intercepted.countDown();
			return record.value().equals("baz") ? null : record;
		});
		container.start();

		ContainerTestUtils.waitForAssignment(container, embeddedKafka.getPartitionsPerTopic());
		assertThat(container.getAssignedPartitions()).hasSize(2);
		Map<String, Collection<TopicPartition>> assignments = container.getAssignmentsByClientId();
		assertThat(assignments).hasSize(2);
		assertThat(assignments.get("client-0")).isNotNull();
		assertThat(assignments.get("client-1")).isNotNull();

		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		ProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf);
		template.setDefaultTopic(topic1);
		template.sendDefault(0, 0, "foo");
		template.sendDefault(1, 2, "bar");
		template.sendDefault(0, 0, "baz");
		template.sendDefault(1, 2, "qux");
		template.flush();
		assertThat(intercepted.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(latch.await(60, TimeUnit.SECONDS)).isTrue();
		synchronized (payloads) {
			assertThat(payloads).containsExactlyInAnyOrder("foo", "bar", "qux");
		}
		assertThat(listenerThreadNames).contains("testAuto-0", "testAuto-1");
		List<KafkaMessageListenerContainer<Integer, String>> containers = KafkaTestUtils.getPropertyValue(container,
				"containers", List.class);
		assertThat(containers).hasSize(2);
		for (int i = 0; i < 2; i++) {
			assertThat(KafkaTestUtils.getPropertyValue(containers.get(i), "listenerConsumer.acks", Collection.class)
					.size()).isEqualTo(0);
		}
		assertThat(container.metrics()).isNotNull();
		Set<KafkaMessageListenerContainer<Integer, String>> children = new HashSet<>(containers);
		assertThat(container.isInExpectedState()).isTrue();
		MessageListenerContainer childContainer = container.getContainers().get(0);
		container.getContainers().get(0).stopAbnormally(() -> {
		});
		assertThat(container.isInExpectedState()).isFalse();
		container.getContainers().get(0).start();
		container.stop();
		assertThat(stopLatch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(concurrentContainerStopLatch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(container.isInExpectedState()).isTrue();
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
			else if (e instanceof ConcurrentContainerStoppedEvent concurrentContainerStoppedEvent) {
				assertThat(concurrentContainerStoppedEvent.getSource()).isSameAs(container);
				assertThat(concurrentContainerStoppedEvent.getContainer(MessageListenerContainer.class))
						.isSameAs(container);
				assertThat(concurrentContainerStoppedEvent.getReason()).isEqualTo(ConsumerStoppedEvent.Reason.NORMAL);
			}
			else {
				assertThat(children).contains((KafkaMessageListenerContainer<Integer, String>) e.getSource());
			}
		});
		assertThat(overrides.get().getProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG)).isNull();
		this.logger.info("Stop auto");
		assertThat(childContainer.isRunning()).isFalse();
		assertThat(container.isRunning()).isFalse();
		// Fenced container. Throws exception
		assertThatExceptionOfType(IllegalStateException.class).isThrownBy(() -> childContainer.start());
	}

	@Test
	public void testAutoCommitWithRebalanceListener() throws Exception {
		this.logger.info("Start auto");
		Map<String, Object> props = KafkaTestUtils.consumerProps(embeddedKafka, "test10", false);
		AtomicReference<Properties> overrides = new AtomicReference<>();
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<Integer, String>(props) {

			@Override
			protected Consumer<Integer, String> createKafkaConsumer(@Nullable String groupId, @Nullable String clientIdPrefix,
					@Nullable String clientIdSuffixArg, @Nullable Properties properties) {

				overrides.set(properties);
				Consumer<Integer, String> created = super.createKafkaConsumer(groupId, clientIdPrefix,
						clientIdSuffixArg, properties);
				assertThat(KafkaTestUtils.getPropertyValue(created, "delegate.requestTimeoutMs", Integer.class)).isEqualTo(23000);
				return created;
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
		Properties nestedProps = new Properties();
		nestedProps.put("weCantAccessThisOne", 42);
		Properties consumerProperties = new Properties(nestedProps);
		consumerProperties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
		consumerProperties.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 23000);
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
		Map<String, Object> props = KafkaTestUtils.consumerProps(embeddedKafka, "test2", false);
		props.remove(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG);
		AtomicReference<Properties> overrides = new AtomicReference<>();
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<Integer, String>(props) {

			@Override
			protected Consumer<Integer, String> createKafkaConsumer(String groupId, String clientIdPrefix,
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

		Set<String> listenerThreadNames = new ConcurrentSkipListSet<>();
		CountDownLatch rebalLatch = new CountDownLatch(5);
		containerProps.setConsumerRebalanceListener(new ConsumerAwareRebalanceListener() {

			@Override
			public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
				if (listenerThreadNames.add(Thread.currentThread().getName())) {
					rebalLatch.countDown();
				}
			}

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
		container.setConcurrency(3);
		container.start();
		assertThat(rebalLatch.await(10, TimeUnit.SECONDS)).isTrue();
		container.stop();
		assertThat(listenerThreadNames)
				.extracting(str -> str.substring(str.length() - 5))
				.containsExactlyInAnyOrder("0-C-1", "1-C-1", "0-C-2", "1-C-2", "2-C-1");
		assertThat(overrides.get().getProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG)).isEqualTo("false");
		this.logger.info("Stop manual");
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
		Map<String, Object> props = KafkaTestUtils.consumerProps(embeddedKafka, "test" + ackMode + qual, false);
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
		Map<String, Object> props = KafkaTestUtils.consumerProps(embeddedKafka, "testManualExisting", false);
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
		Map<String, Object> props = KafkaTestUtils.consumerProps(embeddedKafka, "testManualExistingSync", false);
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
		containerProps.setClientId("myClientId");

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
		Set<String> clientIds = container.getAssignmentsByClientId().keySet();
		assertThat(clientIds).hasSize(1);
		assertThat(clientIds.iterator().next()).isEqualTo("myClientId-0");
		container.stop();
		this.logger.info("Stop MANUAL_IMMEDIATE with Existing");
	}

	@Test
	public void testPausedStart() throws Exception {
		this.logger.info("Start paused start");
		Map<String, Object> props = KafkaTestUtils.consumerProps(embeddedKafka, "test12", false);
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(props);
		ContainerProperties containerProps = new ContainerProperties(topic12);

		final CountDownLatch latch = new CountDownLatch(2);
		containerProps.setMessageListener((MessageListener<Integer, String>) message -> {
			ConcurrentMessageListenerContainerTests.this.logger.info("paused start: " + message);
			latch.countDown();
		});
		containerProps.setClientId("myClientId");
		ConcurrentMessageListenerContainer<Integer, String> container =
				new ConcurrentMessageListenerContainer<>(cf, containerProps);
		container.setConcurrency(2);
		container.setAlwaysClientIdSuffix(false);
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
		Set<String> clientIds = container.getAssignmentsByClientId().keySet();
		assertThat(clientIds).hasSize(2);
		Iterator<String> iterator = clientIds.iterator();
		assertThat(iterator.next()).startsWith("myClientId-");
		assertThat(iterator.next()).startsWith("myClientId-");
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
		containerProps.setMessageListener((MessageListener<Integer, String>) message -> {
		});
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

	@SuppressWarnings("deprecation")
	@Test
	public void testListenerException() throws Exception {
		this.logger.info("Start exception");
		Map<String, Object> props = KafkaTestUtils.consumerProps(embeddedKafka, "test1", true);
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
		container.setCommonErrorHandler(new CommonErrorHandler() {

			@Override
			public boolean handleOne(Exception thrownException, ConsumerRecord<?, ?> record, Consumer<?, ?> consumer,
					MessageListenerContainer container) {

				catchError.set(true);
				return true;
			}
		});
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
		Map<String, Object> props = KafkaTestUtils.consumerProps(embeddedKafka, "test9", false);
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
		ConcurrentMessageListenerContainer<Integer, String> container = new ConcurrentMessageListenerContainer<>(cf,
				containerProps);
		container.setConcurrency(2);
		container.setBeanName("testAckOnError");
		container.setCommonErrorHandler(new CommonErrorHandler() {

			@Override
			public boolean isAckAfterHandle() {
				return false;
			}

			@Override
			public boolean seeksAfterHandling() {
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
	public void testAckOnErrorManualImmediate() throws Exception {
		//ackOnError should not affect manual commits
		testAckOnErrorWithManualImmediateGuts(topic10, true);
		testAckOnErrorWithManualImmediateGuts(topic11, false);
	}

	private void testAckOnErrorWithManualImmediateGuts(String topic, boolean ackOnError) throws Exception {
		logger.info("Start ack on error with ManualImmediate ack mode");
		Map<String, Object> props = KafkaTestUtils.consumerProps(embeddedKafka, "testMan" + ackOnError, false);
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<Integer, String>(props);
		final CountDownLatch latch = new CountDownLatch(2);
		ContainerProperties containerProps = new ContainerProperties(topic);
		containerProps.setSyncCommits(true);
		containerProps.setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
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
		containerProps.setClientId("myClientId");
		ConcurrentMessageListenerContainer<Integer, String> container = new ConcurrentMessageListenerContainer<>(cf,
				containerProps);
		container.setConcurrency(1);
		container.setAlwaysClientIdSuffix(false);
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
		Set<String> clientIds = container.getAssignmentsByClientId().keySet();
		assertThat(clientIds).hasSize(1);
		assertThat(clientIds.iterator().next()).isEqualTo("myClientId");
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

	@Test
	public void testIsChildRunning() throws Exception {
		this.logger.info("Start isChildRunning");
		Map<String, Object> props = KafkaTestUtils.consumerProps(embeddedKafka, "test1", true);
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(props) {

			@Override
			protected Consumer<Integer, String> createKafkaConsumer(String groupId, String clientIdPrefix,
					String clientIdSuffixArg, Properties properties) {
				return super.createKafkaConsumer(groupId, clientIdPrefix, clientIdSuffixArg, properties);
			}
		};
		ContainerProperties containerProps = new ContainerProperties(topic13);
		containerProps.setLogContainerConfig(true);
		containerProps.setClientId("client");
		containerProps.setAckMode(ContainerProperties.AckMode.RECORD);

		final CountDownLatch secondRunLatch = new CountDownLatch(5);
		final Set<String> listenerThreadNames = new ConcurrentSkipListSet<>();
		final List<String> payloads = new ArrayList<>();
		final CountDownLatch processingLatch = new CountDownLatch(1);
		final CountDownLatch firstLatch = new CountDownLatch(1);

		AtomicBoolean first = new AtomicBoolean(true);

		containerProps.setMessageListener((MessageListener<Integer, String>) message -> {
			if (first.getAndSet(false)) {
				try {
					firstLatch.await(100, TimeUnit.SECONDS);
				}
				catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
			}
			ConcurrentMessageListenerContainerTests.this.logger.info("auto: " + message);
			listenerThreadNames.add(Thread.currentThread().getName());
			payloads.add(message.value());
			secondRunLatch.countDown();
			processingLatch.countDown();
		});

		ConcurrentMessageListenerContainer<Integer, String> container =
				new ConcurrentMessageListenerContainer<>(cf, containerProps);
		container.setConcurrency(2);
		container.setBeanName("testAuto");
		container.setChangeConsumerThreadName(true);
		BlockingQueue<KafkaEvent> events = new LinkedBlockingQueue<>();
		CountDownLatch concurrentContainerStopLatch = new CountDownLatch(1);
		CountDownLatch concurrentContainerSecondStopLatch = new CountDownLatch(1);
		CountDownLatch consumerStoppedEventLatch = new CountDownLatch(1);

		container.setApplicationEventPublisher(e -> {
			events.add((KafkaEvent) e);
			if (e instanceof ConcurrentContainerStoppedEvent) {
				concurrentContainerStopLatch.countDown();
				concurrentContainerSecondStopLatch.countDown();
			}
			if (e instanceof ConsumerStoppedEvent) {
				consumerStoppedEventLatch.countDown();
			}
		});

		CountDownLatch interceptedSecondRun = new CountDownLatch(5);
		container.setRecordInterceptor((record, consumer) -> {
			interceptedSecondRun.countDown();
			return record;
		});

		container.start();

		MessageListenerContainer childContainer0 = container.getContainers().get(0);
		MessageListenerContainer childContainer1 = container.getContainers().get(1);

		ContainerTestUtils.waitForAssignment(container, embeddedKafka.getPartitionsPerTopic());
		assertThat(container.getAssignedPartitions()).hasSize(2);
		Map<String, Collection<TopicPartition>> assignments = container.getAssignmentsByClientId();
		assertThat(assignments).hasSize(2);
		assertThat(assignments.get("client-0")).isNotNull();
		assertThat(assignments.get("client-1")).isNotNull();

		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		ProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf);
		template.setDefaultTopic(topic13);
		template.sendDefault(0, 0, "foo");
		template.sendDefault(1, 2, "bar");
		template.sendDefault(0, 0, "baz");
		template.sendDefault(1, 2, "qux");
		template.flush();

		assertThat(container.metrics()).isNotNull();
		assertThat(container.isInExpectedState()).isTrue();
		assertThat(childContainer0.isRunning()).isTrue();
		assertThat(childContainer1.isRunning()).isTrue();
		assertThat(container.isChildRunning()).isTrue();

		assertThat(processingLatch.await(60, TimeUnit.SECONDS)).isTrue();

		container.stop();

		assertThat(container.isChildRunning()).isTrue();
		assertThat(container.isRunning()).isFalse();
		assertThat(childContainer0.isRunning()).isFalse();
		assertThat(childContainer1.isRunning()).isFalse();

		assertThat(consumerStoppedEventLatch.await(30, TimeUnit.SECONDS)).isTrue();

		// This returns true since one container is still processing message. Key validation for this test case.
		assertThat(container.isChildRunning()).isTrue();

		firstLatch.countDown();

		assertThat(listenerThreadNames).containsAnyOf("testAuto-0", "testAuto-1");

		assertThat(concurrentContainerStopLatch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(container.isInExpectedState()).isTrue();
		events.forEach(e -> {
			assertThat(e.getContainer(MessageListenerContainer.class)).isSameAs(container);
			if (e instanceof ConcurrentContainerStoppedEvent concurrentContainerStoppedEvent) {
				assertThat(concurrentContainerStoppedEvent.getSource()).isSameAs(container);
				assertThat(concurrentContainerStoppedEvent.getContainer(MessageListenerContainer.class))
						.isSameAs(container);
				assertThat(concurrentContainerStoppedEvent.getReason()).
						isEqualTo(ConsumerStoppedEvent.Reason.NORMAL);
			}
		});
		assertThat(container.isChildRunning()).isFalse();
		assertThat(payloads).containsAnyOf("foo", "bar", "qux", "baz");

		template.sendDefault(0, 0, "FOO");
		template.sendDefault(1, 2, "BAR");
		template.sendDefault(0, 0, "BAZ");
		template.sendDefault(1, 2, "QUX");
		template.flush();

		container.start();

		assertThat(secondRunLatch.await(60, TimeUnit.SECONDS)).isTrue();
		assertThat(interceptedSecondRun.await(10, TimeUnit.SECONDS)).isTrue();

		container.stop();
		assertThat(concurrentContainerSecondStopLatch.await(30, TimeUnit.SECONDS)).isTrue();
		assertThat(payloads).containsAnyOf("FOO", "BAR", "QUX", "BAZ");

		this.logger.info("Stop isChildRunning");
	}

	@Test
	public void testContainerStartStop() throws Exception {
		this.logger.info("Start containerStartStop");
		Map<String, Object> props = KafkaTestUtils.consumerProps(embeddedKafka, "test1", true);
		AtomicReference<Properties> overrides = new AtomicReference<>();
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<Integer, String>(props) {

			@Override
			protected Consumer<Integer, String> createKafkaConsumer(String groupId, String clientIdPrefix,
					String clientIdSuffixArg, Properties properties) {
				overrides.set(properties);
				return super.createKafkaConsumer(groupId, clientIdPrefix, clientIdSuffixArg, properties);
			}
		};
		ContainerProperties containerProps = new ContainerProperties(topic1);
		containerProps.setLogContainerConfig(true);
		containerProps.setClientId("client");
		containerProps.setAckMode(ContainerProperties.AckMode.RECORD);

		final List<String> payloads = new ArrayList<>();

		containerProps.setMessageListener((MessageListener<Integer, String>) message -> {
			payloads.add(message.value());
		});

		ConcurrentMessageListenerContainer<Integer, String> container =
				new ConcurrentMessageListenerContainer<>(cf, containerProps);
		container.setConcurrency(2);
		container.setBeanName("testAuto");
		container.setChangeConsumerThreadName(true);
		BlockingQueue<KafkaEvent> events = new LinkedBlockingQueue<>();
		CountDownLatch concurrentContainerStopLatch = new CountDownLatch(1);
		CountDownLatch concurrentContainerSecondStopLatch = new CountDownLatch(2);
		CountDownLatch consumerStoppedEventLatch = new CountDownLatch(1);

		container.setApplicationEventPublisher(e -> {
			events.add((KafkaEvent) e);
			if (e instanceof ConcurrentContainerStoppedEvent) {
				concurrentContainerStopLatch.countDown();
				concurrentContainerSecondStopLatch.countDown();
			}
			if (e instanceof ConsumerStoppedEvent) {
				consumerStoppedEventLatch.countDown();
			}
		});

		container.setCommonErrorHandler(null);

		container.start();

		KafkaMessageListenerContainer<Integer, String> childContainer0 = container.getContainers().get(0);
		KafkaMessageListenerContainer<Integer, String> childContainer1 = container.getContainers().get(1);

		ContainerTestUtils.waitForAssignment(container, embeddedKafka.getPartitionsPerTopic());
		assertThat(container.getAssignedPartitions()).hasSize(2);
		Map<String, Collection<TopicPartition>> assignments = container.getAssignmentsByClientId();
		assertThat(assignments).hasSize(2);
		assertThat(assignments.get("client-0")).isNotNull();
		assertThat(assignments.get("client-1")).isNotNull();

		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		ProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(senderProps);

		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf);
		template.setDefaultTopic(topic1);
		template.sendDefault(0, 0, "foo");
		template.sendDefault(1, 2, "bar");
		template.sendDefault(0, 0, "baz");
		template.sendDefault(1, 2, "qux");
		template.flush();

		assertThat(container.metrics()).isNotNull();
		assertThat(container.isInExpectedState()).isTrue();
		assertThat(childContainer0.isRunning()).isTrue();
		assertThat(childContainer1.isRunning()).isTrue();
		assertThat(container.isChildRunning()).isTrue();

		childContainer0.stop();

		assertThat(consumerStoppedEventLatch.await(30, TimeUnit.SECONDS)).isTrue();

		assertThat(container.isChildRunning()).isTrue();
		assertThat(childContainer1.isRunning()).isTrue();
		assertThat(childContainer0.isRunning()).isFalse();
		assertThat(container.isRunning()).isTrue();

		//Ignore this start
		container.start();

		assertThat(container.isChildRunning()).isTrue();
		assertThat(childContainer1.isRunning()).isTrue();
		assertThat(childContainer0.isRunning()).isFalse();
		assertThat(container.getContainers()).
				contains(childContainer0);
		assertThat(container.getContainers()).
				contains(childContainer1);

		container.stop();

		assertThat(container.isRunning()).isFalse();
		// child container1 is stopped.
		assertThat(childContainer1.isRunning()).isFalse();
		assertThat(childContainer0.isRunning()).isFalse();

		assertThat(concurrentContainerStopLatch.await(30, TimeUnit.SECONDS)).isTrue();

		assertThat(container.getContainers()).
				doesNotContain(childContainer0);
		assertThat(container.getContainers()).
				doesNotContain(childContainer1);

		// Accept this start
		container.start();
		assertThat(container.getContainers()).
				doesNotContain(childContainer0);
		assertThat(container.getContainers()).
				doesNotContain(childContainer1);

		KafkaMessageListenerContainer<Integer, String> childContainer0SecRun = container.getContainers().get(0);
		KafkaMessageListenerContainer<Integer, String> childContainer1SecRun = container.getContainers().get(1);

		childContainer0SecRun.stopAbnormally(() -> {
		});

		childContainer1SecRun.stop();

		assertThat(container.getContainers()).isNotEmpty();
		container.stop();
		assertThat(concurrentContainerSecondStopLatch.await(30, TimeUnit.SECONDS)).isTrue();

		events.stream().forEach(event -> {
			if (event.getContainer(MessageListenerContainer.class).equals(childContainer0SecRun)
					&& event instanceof ConsumerStoppedEvent) {
				assertThat(((ConsumerStoppedEvent) event).getReason()).isEqualTo(ConsumerStoppedEvent.Reason.ABNORMAL);
			}
			else if (event.getContainer(MessageListenerContainer.class).equals(childContainer1SecRun)
					&& event instanceof ConsumerStoppedEvent) {
				assertThat(((ConsumerStoppedEvent) event).getReason()).isEqualTo(ConsumerStoppedEvent.Reason.NORMAL);
			}
		});

		this.logger.info("Stop containerStartStop");
	}

}
