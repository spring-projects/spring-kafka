/*
 * Copyright 2016-2018 the original author or authors.
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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;

import org.springframework.beans.DirectFieldAccessor;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.event.ConsumerPausedEvent;
import org.springframework.kafka.event.ConsumerResumedEvent;
import org.springframework.kafka.event.ConsumerStoppedEvent;
import org.springframework.kafka.event.ConsumerStoppingEvent;
import org.springframework.kafka.event.NonResponsiveConsumerEvent;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.springframework.kafka.listener.adapter.FilteringMessageListenerAdapter;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.TopicPartitionInitialOffset;
import org.springframework.kafka.support.TopicPartitionInitialOffset.SeekPosition;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer2;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;

/**
 * Tests for the listener container.
 *
 * @author Gary Russell
 * @author Martin Dam
 * @author Artem Bilan
 * @author Loic Talhouarne
 */
public class KafkaMessageListenerContainerTests {

	private final Log logger = LogFactory.getLog(this.getClass());

	private static String topic1 = "testTopic1";

	private static String topic2 = "testTopic2";

	private static String topic3 = "testTopic3";

	private static String topic4 = "testTopic4";

	private static String topic5 = "testTopic5";

	private static String topic6 = "testTopic6";

	private static String topic7 = "testTopic7";

	private static String topic8 = "testTopic8";

	private static String topic9 = "testTopic9";

	private static String topic10 = "testTopic10";

	private static String topic11 = "testTopic11";

	private static String topic12 = "testTopic12";

	private static String topic13 = "testTopic13";

	private static String topic14 = "testTopic14";

	private static String topic15 = "testTopic15";

	private static String topic16 = "testTopic16";

	private static String topic17 = "testTopic17";

	private static String topic18 = "testTopic18";

	private static String topic19 = "testTopic19";

	private static String topic20 = "testTopic20";

	private static String topic21 = "testTopic21";


	@ClassRule
	public static EmbeddedKafkaRule embeddedKafkaRule = new EmbeddedKafkaRule(1, true, topic1, topic2, topic3, topic4,
			topic5, topic6, topic7, topic8, topic9, topic10, topic11, topic12, topic13, topic14, topic15, topic16,
			topic17, topic18, topic19, topic20, topic21);

	private static EmbeddedKafkaBroker embeddedKafka = embeddedKafkaRule.getEmbeddedKafka();

	@Rule
	public TestName testName = new TestName();

	@Test
	public void testDelegateType() throws Exception {
		Map<String, Object> props = KafkaTestUtils.consumerProps("delegate", "false", embeddedKafka);
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(props);
		ContainerProperties containerProps = new ContainerProperties(topic3);
		containerProps.setShutdownTimeout(60_000L);
		final AtomicReference<StackTraceElement[]> trace = new AtomicReference<>();
		final CountDownLatch latch1 = new CountDownLatch(1);
		containerProps.setMessageListener((MessageListener<Integer, String>) record -> {
			trace.set(new RuntimeException().getStackTrace());
			latch1.countDown();
		});
		KafkaMessageListenerContainer<Integer, String> container =
				new KafkaMessageListenerContainer<>(cf, containerProps);
		container.setBeanName("delegate");
		AtomicReference<List<TopicPartitionInitialOffset>> offsets = new AtomicReference<>();
		container.setApplicationEventPublisher(e -> {
			if (e instanceof ConsumerStoppingEvent) {
				ConsumerStoppingEvent event = (ConsumerStoppingEvent) e;
				offsets.set(event.getPartitions().stream()
						.map(p -> new TopicPartitionInitialOffset(p.topic(), p.partition(),
								event.getConsumer().position(p, Duration.ofMillis(10_000))))
						.collect(Collectors.toList()));
			}
		});
		container.start();

		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		DefaultKafkaProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf);
		template.setDefaultTopic(topic3);
		template.sendDefault(0, 0, "foo");
		template.flush();

		assertThat(latch1.await(10, TimeUnit.SECONDS)).isTrue();
		// Stack traces are environment dependent - verified in eclipse
		//		assertThat(trace.get()[1].getMethodName()).contains("invokeRecordListener");
		container.stop();
		List<TopicPartitionInitialOffset> list = offsets.get();
		assertThat(list).isNotNull();
		list.forEach(tpio -> {
				if (tpio.partition() == 0) {
					assertThat(tpio.initialOffset()).isEqualTo(1);
				}
				else {
					assertThat(tpio.initialOffset()).isEqualTo(0);
				}
		});
		final CountDownLatch latch2 = new CountDownLatch(1);
		FilteringMessageListenerAdapter<Integer, String> filtering = new FilteringMessageListenerAdapter<>(m -> {
			trace.set(new RuntimeException().getStackTrace());
			latch2.countDown();
		}, d -> false);
		filtering = new FilteringMessageListenerAdapter<>(filtering, d -> false); // two levels of nesting
		container.getContainerProperties().setMessageListener(filtering);
		container.start();
		assertThat(KafkaTestUtils.getPropertyValue(container, "listenerConsumer.listenerType"))
				.isEqualTo(ListenerType.SIMPLE);
		template.sendDefault(0, 0, "foo");
		assertThat(latch2.await(10, TimeUnit.SECONDS)).isTrue();
		// verify that the container called the right method - avoiding the creation of an Acknowledgment
		//		assertThat(trace.get()[1].getMethodName()).contains("onMessage"); // onMessage(d, a, c) (inner)
		//		assertThat(trace.get()[2].getMethodName()).contains("onMessage"); // bridge
		//		assertThat(trace.get()[3].getMethodName()).contains("onMessage"); // onMessage(d, a, c) (outer)
		//		assertThat(trace.get()[4].getMethodName()).contains("onMessage"); // onMessage(d)
		//		assertThat(trace.get()[5].getMethodName()).contains("onMessage"); // bridge
		//		assertThat(trace.get()[6].getMethodName()).contains("invokeRecordListener");
		container.stop();
		final CountDownLatch latch3 = new CountDownLatch(1);
		filtering = new FilteringMessageListenerAdapter<>(
				(AcknowledgingConsumerAwareMessageListener<Integer, String>) (d, a, c) -> {
					trace.set(new RuntimeException().getStackTrace());
					latch3.countDown();
				}, d -> false);
		container.getContainerProperties().setMessageListener(filtering);
		container.start();
		assertThat(KafkaTestUtils.getPropertyValue(container, "listenerConsumer.listenerType"))
				.isEqualTo(ListenerType.ACKNOWLEDGING_CONSUMER_AWARE);
		template.sendDefault(0, 0, "foo");
		assertThat(latch3.await(10, TimeUnit.SECONDS)).isTrue();
		// verify that the container called the 3 arg method directly
		//		int i = 0;
		//		if (trace.get()[1].getClassName().endsWith("AcknowledgingConsumerAwareMessageListener")) {
		//			// this frame does not appear in eclise, but does in gradle.\
		//			i++;
		//		}
		//		assertThat(trace.get()[i + 1].getMethodName()).contains("onMessage"); // onMessage(d, a, c)
		//		assertThat(trace.get()[i + 2].getMethodName()).contains("onMessage"); // bridge
		//		assertThat(trace.get()[i + 3].getMethodName()).contains("invokeRecordListener");
		container.stop();
		long t = System.currentTimeMillis();
		container.stop();
		assertThat(System.currentTimeMillis() - t).isLessThan(5000L);

		pf.destroy();
	}

	@Test
	public void testNoResetPolicy() throws Exception {
		Map<String, Object> props = KafkaTestUtils.consumerProps("delegate", "false", embeddedKafka);
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(props);
		ContainerProperties containerProps = new ContainerProperties(topic17);
		final AtomicReference<StackTraceElement[]> trace = new AtomicReference<>();
		final CountDownLatch latch1 = new CountDownLatch(1);
		containerProps.setMessageListener((MessageListener<Integer, String>) record -> {
			trace.set(new RuntimeException().getStackTrace());
			latch1.countDown();
		});
		KafkaMessageListenerContainer<Integer, String> container =
				new KafkaMessageListenerContainer<>(cf, containerProps);
		container.setBeanName("delegate");
		container.start();

		int n = 0;
		while (n++ < 200 && container.isRunning()) {
			Thread.sleep(100);
		}
		assertThat(container.isRunning()).isFalse();
	}

	@Test
	public void testListenerTypes() throws Exception {
		Map<String, Object> props = KafkaTestUtils.consumerProps("lt1", "false", embeddedKafka);
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(props);
		ContainerProperties containerProps = new ContainerProperties(topic4);

		final CountDownLatch latch = new CountDownLatch(8);
		final BitSet bits = new BitSet(8);
		containerProps.setMessageListener((MessageListener<Integer, String>) m -> {
			this.logger.info("lt1");
			synchronized (bits) {
				bits.set(0);
			}
			latch.countDown();
		});
		KafkaMessageListenerContainer<Integer, String> container1 =
				new KafkaMessageListenerContainer<>(cf, containerProps);
		container1.setBeanName("lt1");
		container1.start();

		props.put("group.id", "lt2");
		cf = new DefaultKafkaConsumerFactory<>(props);
		containerProps.setMessageListener((AcknowledgingMessageListener<Integer, String>) (m, a) -> {
			this.logger.info("lt2");
			synchronized (bits) {
				bits.set(1);
			}
			latch.countDown();
		});
		KafkaMessageListenerContainer<Integer, String> container2 =
				new KafkaMessageListenerContainer<>(cf, containerProps);
		container2.setBeanName("lt2");
		container2.start();

		props.put("group.id", "lt3");
		cf = new DefaultKafkaConsumerFactory<>(props);
		containerProps.setMessageListener((ConsumerAwareMessageListener<Integer, String>) (m, c) -> {
			this.logger.info("lt3");
			synchronized (bits) {
				bits.set(2);
			}
			latch.countDown();
		});
		KafkaMessageListenerContainer<Integer, String> container3 =
				new KafkaMessageListenerContainer<>(cf, containerProps);
		container3.setBeanName("lt3");
		container3.start();

		props.put("group.id", "lt4");
		cf = new DefaultKafkaConsumerFactory<>(props);
		containerProps.setMessageListener((AcknowledgingConsumerAwareMessageListener<Integer, String>) (m, a, c) -> {
			this.logger.info("lt4");
			synchronized (bits) {
				bits.set(3);
			}
			latch.countDown();
		});
		KafkaMessageListenerContainer<Integer, String> container4 =
				new KafkaMessageListenerContainer<>(cf, containerProps);
		container4.setBeanName("lt4");
		container4.start();

		props.put("group.id", "lt5");
		cf = new DefaultKafkaConsumerFactory<>(props);
		containerProps.setMessageListener((BatchMessageListener<Integer, String>) m -> {
			this.logger.info("lt5");
			synchronized (bits) {
				bits.set(4);
			}
			latch.countDown();
		});
		KafkaMessageListenerContainer<Integer, String> container5 =
				new KafkaMessageListenerContainer<>(cf, containerProps);
		container5.setBeanName("lt5");
		container5.start();

		props.put("group.id", "lt6");
		cf = new DefaultKafkaConsumerFactory<>(props);
		containerProps.setMessageListener((BatchAcknowledgingMessageListener<Integer, String>) (m, a) -> {
			this.logger.info("lt6");
			synchronized (bits) {
				bits.set(5);
			}
			latch.countDown();
		});
		KafkaMessageListenerContainer<Integer, String> container6 =
				new KafkaMessageListenerContainer<>(cf, containerProps);
		container6.setBeanName("lt6");
		container6.start();

		props.put("group.id", "lt7");
		cf = new DefaultKafkaConsumerFactory<>(props);
		containerProps.setMessageListener((BatchConsumerAwareMessageListener<Integer, String>) (m, c) -> {
			this.logger.info("lt7");
			synchronized (bits) {
				bits.set(6);
			}
			latch.countDown();
		});
		KafkaMessageListenerContainer<Integer, String> container7 =
				new KafkaMessageListenerContainer<>(cf, containerProps);
		container7.setBeanName("lt7");
		container7.start();

		props.put("group.id", "lt8");
		cf = new DefaultKafkaConsumerFactory<>(props);
		containerProps
				.setMessageListener((BatchAcknowledgingConsumerAwareMessageListener<Integer, String>) (m, a, c) -> {
					this.logger.info("lt8");
					synchronized (bits) {
						bits.set(7);
					}
					latch.countDown();
				});
		KafkaMessageListenerContainer<Integer, String> container8 =
				new KafkaMessageListenerContainer<>(cf, containerProps);
		container8.setBeanName("lt8");
		container8.start();

		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		DefaultKafkaProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf);
		template.setDefaultTopic(topic4);
		template.sendDefault(0, 0, "foo");
		template.flush();
		pf.destroy();

		assertThat(latch.await(30, TimeUnit.SECONDS)).isTrue();
		assertThat(bits.cardinality()).isEqualTo(8);

		container1.stop();
		container2.stop();
		container3.stop();
		container4.stop();
		container5.stop();
		container6.stop();
		container7.stop();
		container8.stop();
	}

	@Test
	public void testCommitsAreFlushedOnStop() throws Exception {
		Map<String, Object> props = KafkaTestUtils.consumerProps("flushedOnStop", "false", embeddedKafka);
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(props);
		ContainerProperties containerProps = new ContainerProperties(topic5);
		containerProps.setAckCount(1);
		// set large values, ensuring that commits don't happen before `stop()`
		containerProps.setAckTime(20000);
		containerProps.setAckCount(20000);
		containerProps.setAckMode(AckMode.COUNT_TIME);

		final CountDownLatch latch = new CountDownLatch(4);
		containerProps.setMessageListener((MessageListener<Integer, String>) message -> {
			logger.info("flushed: " + message);
			latch.countDown();
		});
		KafkaMessageListenerContainer<Integer, String> container =
				new KafkaMessageListenerContainer<>(cf, containerProps);
		container.setBeanName("testManualFlushed");

		container.start();
		Consumer<?, ?> consumer = spyOnConsumer(container);
		ContainerTestUtils.waitForAssignment(container, embeddedKafka.getPartitionsPerTopic());

		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		ProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf);
		template.setDefaultTopic(topic5);
		template.sendDefault(0, 0, "foo");
		template.sendDefault(1, 2, "bar");
		template.flush();
		Thread.sleep(300);
		template.sendDefault(0, 0, "fiz");
		template.sendDefault(1, 2, "buz");
		template.flush();

		// Verify that commitSync is called when paused
		assertThat(latch.await(60, TimeUnit.SECONDS)).isTrue();
		// Verify that just the initial commit is processed before stop
		verify(consumer, times(1)).commitSync(anyMap());
		container.stop();
		// Verify that a commit has been made on stop
		verify(consumer, times(2)).commitSync(anyMap());
	}

	@Test
	public void testRecordAck() throws Exception {
		logger.info("Start record ack");
		Map<String, Object> props = KafkaTestUtils.consumerProps("test6", "false", embeddedKafka);
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(props);
		ContainerProperties containerProps = new ContainerProperties(topic6);
		containerProps.setMessageListener((MessageListener<Integer, String>) message -> {
			logger.info("record ack: " + message);
		});
		containerProps.setSyncCommits(true);
		containerProps.setAckMode(AckMode.RECORD);
		containerProps.setAckOnError(false);
		//		containerProps.setCommitLogLevel(LogIfLevelEnabled.Level.WARN);

		CountDownLatch stubbingComplete = new CountDownLatch(1);
		KafkaMessageListenerContainer<Integer, String> container = spyOnContainer(
				new KafkaMessageListenerContainer<>(cf, containerProps), stubbingComplete);
		container.setBeanName("testRecordAcks");
		container.start();
		Consumer<?, ?> containerConsumer = spyOnConsumer(container);
		final CountDownLatch latch = new CountDownLatch(2);
		willAnswer(invocation -> {

			Map<TopicPartition, OffsetAndMetadata> map = invocation.getArgument(0);
			try {
				return invocation.callRealMethod();
			}
			finally {
				for (Entry<TopicPartition, OffsetAndMetadata> entry : map.entrySet()) {
					if (entry.getValue().offset() == 2) {
						latch.countDown();
					}
				}
			}

		}).given(containerConsumer)
				.commitSync(anyMap());
		stubbingComplete.countDown();
		ContainerTestUtils.waitForAssignment(container, embeddedKafka.getPartitionsPerTopic());
		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		ProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf);
		template.setDefaultTopic(topic6);
		template.sendDefault(0, 0, "foo");
		template.sendDefault(1, 0, "bar");
		template.sendDefault(0, 0, "baz");
		template.sendDefault(1, 0, "qux");
		template.flush();
		assertThat(latch.await(60, TimeUnit.SECONDS)).isTrue();
		Consumer<Integer, String> consumer = cf.createConsumer();
		consumer.assign(Arrays.asList(new TopicPartition(topic6, 0), new TopicPartition(topic6, 1)));
		assertThat(consumer.position(new TopicPartition(topic6, 0))).isEqualTo(2);
		assertThat(consumer.position(new TopicPartition(topic6, 1))).isEqualTo(2);
		container.stop();
		consumer.close();
		logger.info("Stop record ack");
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testRecordAckMock() throws Exception {
		ConsumerFactory<Integer, String> cf = mock(ConsumerFactory.class);
		Consumer<Integer, String> consumer = mock(Consumer.class);
		given(cf.createConsumer(eq("grp"), eq("clientId"), isNull())).willReturn(consumer);
		final Map<TopicPartition, List<ConsumerRecord<Integer, String>>> records = new HashMap<>();
		records.put(new TopicPartition("foo", 0), Arrays.asList(
				new ConsumerRecord<>("foo", 0, 0L, 1, "foo"),
				new ConsumerRecord<>("foo", 0, 1L, 1, "bar")));
		ConsumerRecords<Integer, String> consumerRecords = new ConsumerRecords<>(records);
		given(consumer.poll(any(Duration.class))).willAnswer(i -> {
			Thread.sleep(50);
			return consumerRecords;
		});
		TopicPartitionInitialOffset[] topicPartition = new TopicPartitionInitialOffset[] {
				new TopicPartitionInitialOffset("foo", 0) };
		ContainerProperties containerProps = new ContainerProperties(topicPartition);
		containerProps.setGroupId("grp");
		containerProps.setAckMode(AckMode.RECORD);
		final CountDownLatch latch = new CountDownLatch(2);
		MessageListener<Integer, String> messageListener = spy(
				new MessageListener<Integer, String>() { // Cannot be lambda: Mockito doesn't mock final classes

					@Override
					public void onMessage(ConsumerRecord<Integer, String> data) {
						latch.countDown();
						if (latch.getCount() == 0) {
							records.clear();
						}
					}

				});

		final CountDownLatch commitLatch = new CountDownLatch(2);

		willAnswer(i -> {
					commitLatch.countDown();
					return null;
				}
		).given(consumer).commitSync(any(Map.class));

		containerProps.setMessageListener(messageListener);
		containerProps.setClientId("clientId");
		KafkaMessageListenerContainer<Integer, String> container =
				new KafkaMessageListenerContainer<>(cf, containerProps);
		container.start();
		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(commitLatch.await(10, TimeUnit.SECONDS)).isTrue();
		InOrder inOrder = inOrder(messageListener, consumer);
		inOrder.verify(consumer).poll(Duration.ofMillis(ContainerProperties.DEFAULT_POLL_TIMEOUT));
		inOrder.verify(messageListener).onMessage(any(ConsumerRecord.class));
		inOrder.verify(consumer).commitSync(any(Map.class));
		inOrder.verify(messageListener).onMessage(any(ConsumerRecord.class));
		inOrder.verify(consumer).commitSync(any(Map.class));
		container.stop();
	}

	@Test
	public void testRecordAckMockForeignThread() throws Exception {
		testRecordAckMockForeignThreadGuts(AckMode.MANUAL);
	}

	@Test
	public void testRecordAckMockForeignThreadImmediate() throws Exception {
		testRecordAckMockForeignThreadGuts(AckMode.MANUAL_IMMEDIATE);
	}

	@SuppressWarnings("unchecked")
	private void testRecordAckMockForeignThreadGuts(AckMode ackMode) throws Exception {
		ConsumerFactory<Integer, String> cf = mock(ConsumerFactory.class);
		Consumer<Integer, String> consumer = mock(Consumer.class);
		given(cf.createConsumer(eq("grp"), eq("clientId"), isNull())).willReturn(consumer);
		final Map<TopicPartition, List<ConsumerRecord<Integer, String>>> records = new HashMap<>();
		records.put(new TopicPartition("foo", 0), Arrays.asList(
				new ConsumerRecord<>("foo", 0, 0L, 1, "foo"),
				new ConsumerRecord<>("foo", 0, 1L, 1, "bar")));
		ConsumerRecords<Integer, String> consumerRecords = new ConsumerRecords<>(records);
		given(consumer.poll(any(Duration.class))).willAnswer(i -> {
			Thread.sleep(50);
			return consumerRecords;
		});
		TopicPartitionInitialOffset[] topicPartition = new TopicPartitionInitialOffset[] {
				new TopicPartitionInitialOffset("foo", 0) };
		ContainerProperties containerProps = new ContainerProperties(topicPartition);
		containerProps.setGroupId("grp");
		containerProps.setAckMode(ackMode);
		final CountDownLatch latch = new CountDownLatch(2);
		final List<Acknowledgment> acks = new ArrayList<>();
		final AtomicReference<Thread> consumerThread = new AtomicReference<>();
		AcknowledgingMessageListener<Integer, String> messageListener = spy(
				new AcknowledgingMessageListener<Integer, String>() { // Mockito doesn't mock final classes

					@Override
					public void onMessage(ConsumerRecord<Integer, String> data, Acknowledgment acknowledgment) {
						acks.add(acknowledgment);
						consumerThread.set(Thread.currentThread());
						latch.countDown();
						if (latch.getCount() == 0) {
							records.clear();
						}
					}

				});

		final CountDownLatch commitLatch = new CountDownLatch(1);
		final AtomicReference<Thread> commitThread = new AtomicReference<>();
		willAnswer(i -> {
					commitThread.set(Thread.currentThread());
					commitLatch.countDown();
					return null;
				}
		).given(consumer).commitSync(any(Map.class));

		containerProps.setMessageListener(messageListener);
		containerProps.setClientId("clientId");
		KafkaMessageListenerContainer<Integer, String> container =
				new KafkaMessageListenerContainer<>(cf, containerProps);
		container.start();
		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		acks.get(1).acknowledge();
		assertThat(commitLatch.await(10, TimeUnit.SECONDS)).isTrue();
		InOrder inOrder = inOrder(messageListener, consumer);
		inOrder.verify(consumer).poll(Duration.ofMillis(ContainerProperties.DEFAULT_POLL_TIMEOUT));
		inOrder.verify(messageListener, times(2)).onMessage(any(ConsumerRecord.class), any(Acknowledgment.class));
		inOrder.verify(consumer).commitSync(any(Map.class));
		container.stop();
		assertThat(commitThread.get()).isSameAs(consumerThread.get());
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testNonResponsiveConsumerEvent() throws Exception {
		ConsumerFactory<Integer, String> cf = mock(ConsumerFactory.class);
		Consumer<Integer, String> consumer = mock(Consumer.class);
		given(cf.createConsumer(eq("grp"), eq(""), isNull())).willReturn(consumer);
		final Map<TopicPartition, List<ConsumerRecord<Integer, String>>> records = new HashMap<>();
		records.put(new TopicPartition("foo", 0), Arrays.asList(
				new ConsumerRecord<>("foo", 0, 0L, 1, "foo"),
				new ConsumerRecord<>("foo", 0, 1L, 1, "bar")));
		final CountDownLatch deadLatch = new CountDownLatch(1);
		given(consumer.poll(any(Duration.class))).willAnswer(i -> {
			deadLatch.await(10, TimeUnit.SECONDS);
			throw new WakeupException();
		});
		willAnswer(i -> {
			deadLatch.countDown();
			return null;
		}).given(consumer).wakeup();
		TopicPartitionInitialOffset[] topicPartition = new TopicPartitionInitialOffset[] {
				new TopicPartitionInitialOffset("foo", 0) };
		ContainerProperties containerProps = new ContainerProperties(topicPartition);
		containerProps.setGroupId("grp");
		containerProps.setNoPollThreshold(2.0f);
		containerProps.setPollTimeout(10);
		containerProps.setMonitorInterval(1);
		containerProps.setMessageListener(mock(MessageListener.class));
		KafkaMessageListenerContainer<Integer, String> container =
				new KafkaMessageListenerContainer<>(cf, containerProps);
		final CountDownLatch latch = new CountDownLatch(1);
		container.setApplicationEventPublisher(e -> {
			if (e instanceof NonResponsiveConsumerEvent) {
				latch.countDown();
			}
		});
		container.start();
		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		container.stop();
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testNonResponsiveConsumerEventNotIssuedWithActiveConsumer() throws Exception {
		ConsumerFactory<Integer, String> cf = mock(ConsumerFactory.class);
		Consumer<Integer, String> consumer = mock(Consumer.class);
		given(cf.createConsumer(isNull(), eq(""), isNull())).willReturn(consumer);
		ConsumerRecords records = new ConsumerRecords(Collections.emptyMap());
		CountDownLatch latch = new CountDownLatch(20);
		given(consumer.poll(any(Duration.class))).willAnswer(i -> {
			Thread.sleep(100);
			latch.countDown();
			return records;
		});
		TopicPartitionInitialOffset[] topicPartition = new TopicPartitionInitialOffset[] {
				new TopicPartitionInitialOffset("foo", 0) };
		ContainerProperties containerProps = new ContainerProperties(topicPartition);
		containerProps.setNoPollThreshold(2.0f);
		containerProps.setPollTimeout(100);
		containerProps.setMonitorInterval(1);
		containerProps.setMessageListener(mock(MessageListener.class));
		KafkaMessageListenerContainer<Integer, String> container =
				new KafkaMessageListenerContainer<>(cf, containerProps);
		final AtomicInteger eventCounter = new AtomicInteger();
		container.setApplicationEventPublisher(e -> {
			if (e instanceof NonResponsiveConsumerEvent) {
				eventCounter.incrementAndGet();
			}
		});
		container.start();
		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		container.stop();
		assertThat(eventCounter.get()).isEqualTo(0);
	}

	@Test
	public void testBatchAck() throws Exception {
		logger.info("Start batch ack");

		Map<String, Object> props = KafkaTestUtils.consumerProps("test6", "false", embeddedKafka);
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(props);
		ContainerProperties containerProps = new ContainerProperties(topic7);
		containerProps.setMessageListener((MessageListener<Integer, String>) message -> {
			logger.info("batch ack: " + message);
		});
		containerProps.setSyncCommits(true);
		containerProps.setAckMode(AckMode.BATCH);
		containerProps.setPollTimeout(100);
		containerProps.setAckOnError(false);

		CountDownLatch stubbingComplete = new CountDownLatch(1);
		KafkaMessageListenerContainer<Integer, String> container = spyOnContainer(
				new KafkaMessageListenerContainer<>(cf, containerProps), stubbingComplete);
		container.setBeanName("testBatchAcks");
		container.start();
		Consumer<?, ?> containerConsumer = spyOnConsumer(container);
		final CountDownLatch firstBatchLatch = new CountDownLatch(1);
		final CountDownLatch latch = new CountDownLatch(2);
		willAnswer(invocation -> {

			Map<TopicPartition, OffsetAndMetadata> map = invocation.getArgument(0);
			for (Entry<TopicPartition, OffsetAndMetadata> entry : map.entrySet()) {
				if (entry.getValue().offset() == 2) {
					firstBatchLatch.countDown();
				}
			}
			try {
				return invocation.callRealMethod();
			}
			finally {
				for (Entry<TopicPartition, OffsetAndMetadata> entry : map.entrySet()) {
					if (entry.getValue().offset() == 2) {
						latch.countDown();
					}
				}
			}

		}).given(containerConsumer)
				.commitSync(anyMap());
		stubbingComplete.countDown();

		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		ProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf);
		template.setDefaultTopic(topic7);
		template.sendDefault(0, 0, "foo");
		template.sendDefault(0, 0, "baz");
		template.sendDefault(1, 0, "bar");
		template.sendDefault(1, 0, "qux");
		template.flush();

		assertThat(firstBatchLatch.await(9, TimeUnit.SECONDS)).isTrue();

		assertThat(latch.await(60, TimeUnit.SECONDS)).isTrue();
		Consumer<Integer, String> consumer = cf.createConsumer();
		consumer.assign(Arrays.asList(new TopicPartition(topic7, 0), new TopicPartition(topic7, 1)));
		assertThat(consumer.position(new TopicPartition(topic7, 0))).isEqualTo(2);
		assertThat(consumer.position(new TopicPartition(topic7, 1))).isEqualTo(2);
		container.stop();
		consumer.close();
		logger.info("Stop batch ack");
	}

	@Test
	public void testBatchListener() throws Exception {
		logger.info("Start batch listener");

		Map<String, Object> props = KafkaTestUtils.consumerProps("test8", "false", embeddedKafka);
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(props);
		ContainerProperties containerProps = new ContainerProperties(topic8);
		containerProps.setMessageListener((BatchMessageListener<Integer, String>) messages -> {
			logger.info("batch listener: " + messages);
		});
		containerProps.setSyncCommits(true);
		containerProps.setAckMode(AckMode.BATCH);
		containerProps.setPollTimeout(100);
		containerProps.setAckOnError(false);

		CountDownLatch stubbingComplete = new CountDownLatch(1);
		KafkaMessageListenerContainer<Integer, String> container = spyOnContainer(
				new KafkaMessageListenerContainer<>(cf, containerProps), stubbingComplete);
		container.setBeanName("testBatchListener");
		container.start();
		Consumer<?, ?> containerConsumer = spyOnConsumer(container);
		final CountDownLatch firstBatchLatch = new CountDownLatch(1);
		final CountDownLatch latch = new CountDownLatch(2);
		willAnswer(invocation -> {

			Map<TopicPartition, OffsetAndMetadata> map = invocation.getArgument(0);
			for (Entry<TopicPartition, OffsetAndMetadata> entry : map.entrySet()) {
				if (entry.getValue().offset() == 2) {
					firstBatchLatch.countDown();
				}
			}
			try {
				return invocation.callRealMethod();
			}
			finally {
				for (Entry<TopicPartition, OffsetAndMetadata> entry : map.entrySet()) {
					if (entry.getValue().offset() == 2) {
						latch.countDown();
					}
				}
			}

		}).given(containerConsumer)
				.commitSync(anyMap());
		stubbingComplete.countDown();

		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		ProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf);
		template.setDefaultTopic(topic8);
		template.sendDefault(0, 0, "foo");
		template.sendDefault(0, 0, "baz");
		template.sendDefault(1, 0, "bar");
		template.sendDefault(1, 0, "qux");
		template.flush();

		assertThat(firstBatchLatch.await(9, TimeUnit.SECONDS)).isTrue();

		assertThat(latch.await(60, TimeUnit.SECONDS)).isTrue();
		Consumer<Integer, String> consumer = cf.createConsumer();
		consumer.assign(Arrays.asList(new TopicPartition(topic8, 0), new TopicPartition(topic8, 1)));
		assertThat(consumer.position(new TopicPartition(topic8, 0))).isEqualTo(2);
		assertThat(consumer.position(new TopicPartition(topic8, 1))).isEqualTo(2);
		container.stop();
		consumer.close();
		logger.info("Stop batch listener");
	}

	@Test
	public void testBatchListenerManual() throws Exception {
		logger.info("Start batch listener manual");

		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		ProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf);
		template.setDefaultTopic(topic9);
		template.sendDefault(0, 0, "foo");
		template.sendDefault(0, 0, "baz");
		template.sendDefault(1, 0, "bar");
		template.sendDefault(1, 0, "qux");
		template.flush();

		Map<String, Object> props = KafkaTestUtils.consumerProps("test9", "false", embeddedKafka);
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(props);
		ContainerProperties containerProps = new ContainerProperties(topic9);
		final CountDownLatch latch = new CountDownLatch(4);
		containerProps.setMessageListener((BatchAcknowledgingMessageListener<Integer, String>) (messages, ack) -> {
			logger.info("batch listener manual: " + messages);
			for (int i = 0; i < messages.size(); i++) {
				latch.countDown();
			}
			ack.acknowledge();
		});
		containerProps.setSyncCommits(true);
		containerProps.setAckMode(AckMode.MANUAL_IMMEDIATE);
		containerProps.setPollTimeout(100);
		containerProps.setAckOnError(false);

		CountDownLatch stubbingComplete = new CountDownLatch(1);
		KafkaMessageListenerContainer<Integer, String> container = spyOnContainer(
				new KafkaMessageListenerContainer<>(cf, containerProps), stubbingComplete);
		container.setBeanName("testBatchListenerManual");
		container.start();
		Consumer<?, ?> containerConsumer = spyOnConsumer(container);
		final CountDownLatch commitLatch = new CountDownLatch(2);
		AtomicBoolean smallOffsetCommitted = new AtomicBoolean(false);
		willAnswer(invocation -> {

			Map<TopicPartition, OffsetAndMetadata> map = invocation.getArgument(0);
			try {
				return invocation.callRealMethod();
			}
			finally {
				for (Entry<TopicPartition, OffsetAndMetadata> entry : map.entrySet()) {
					if (entry.getValue().offset() == 1) {
						smallOffsetCommitted.set(true);
					}
					else if (entry.getValue().offset() == 2) {
						commitLatch.countDown();
					}
				}
			}

		}).given(containerConsumer)
				.commitSync(anyMap());
		stubbingComplete.countDown();

		assertThat(latch.await(60, TimeUnit.SECONDS)).isTrue();
		assertThat(commitLatch.await(60, TimeUnit.SECONDS)).isTrue();
		assertThat(smallOffsetCommitted.get()).isFalse();
		Consumer<Integer, String> consumer = cf.createConsumer();
		consumer.assign(Arrays.asList(new TopicPartition(topic9, 0), new TopicPartition(topic9, 1)));
		assertThat(consumer.position(new TopicPartition(topic9, 0))).isEqualTo(2);
		assertThat(consumer.position(new TopicPartition(topic9, 1))).isEqualTo(2);
		container.stop();
		consumer.close();
		logger.info("Stop batch listener manual");
	}

	@Test
	public void testBatchListenerErrors() throws Exception {
		logger.info("Start batch listener errors");

		Map<String, Object> props = KafkaTestUtils.consumerProps("test9", "false", embeddedKafka);
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(props);
		ContainerProperties containerProps = new ContainerProperties(topic10);
		containerProps.setMessageListener((BatchMessageListener<Integer, String>) messages -> {
			logger.info("batch listener errors: " + messages);
			throw new RuntimeException("intentional");
		});
		containerProps.setSyncCommits(true);
		containerProps.setAckMode(AckMode.BATCH);
		containerProps.setPollTimeout(100);
		containerProps.setAckOnError(true);
		final CountDownLatch latch = new CountDownLatch(4);

		CountDownLatch stubbingComplete = new CountDownLatch(1);
		KafkaMessageListenerContainer<Integer, String> container = spyOnContainer(
				new KafkaMessageListenerContainer<>(cf, containerProps), stubbingComplete);
		container.setBeanName("testBatchListenerErrors");
		container.setBatchErrorHandler((t, messages) -> {
			new BatchLoggingErrorHandler().handle(t, messages);
			for (int i = 0; i < messages.count(); i++) {
				latch.countDown();
			}
		});
		container.start();
		Consumer<?, ?> containerConsumer = spyOnConsumer(container);
		final CountDownLatch commitLatch = new CountDownLatch(2);
		willAnswer(invocation -> {

			Map<TopicPartition, OffsetAndMetadata> map = invocation.getArgument(0);
			try {
				return invocation.callRealMethod();
			}
			finally {
				for (Entry<TopicPartition, OffsetAndMetadata> entry : map.entrySet()) {
					if (entry.getValue().offset() == 2) {
						commitLatch.countDown();
					}
				}
			}

		}).given(containerConsumer)
				.commitSync(anyMap());
		stubbingComplete.countDown();

		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		ProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf);
		template.setDefaultTopic(topic10);
		template.sendDefault(0, 0, "foo");
		template.sendDefault(0, 0, "baz");
		template.sendDefault(1, 0, "bar");
		template.sendDefault(1, 0, "qux");
		template.flush();

		assertThat(latch.await(60, TimeUnit.SECONDS)).isTrue();
		assertThat(commitLatch.await(60, TimeUnit.SECONDS)).isTrue();
		Consumer<Integer, String> consumer = cf.createConsumer();
		consumer.assign(Arrays.asList(new TopicPartition(topic10, 0), new TopicPartition(topic10, 1)));
		assertThat(consumer.position(new TopicPartition(topic10, 0))).isEqualTo(2);
		assertThat(consumer.position(new TopicPartition(topic10, 1))).isEqualTo(2);
		container.stop();
		consumer.close();
		logger.info("Stop batch listener errors");
	}

	@Test
	public void testSeek() throws Exception {
		Map<String, Object> props = KafkaTestUtils.consumerProps("test11", "false", embeddedKafka);
		testSeekGuts(props, topic11, false);
	}

	@Test
	public void testSeekAutoCommit() throws Exception {
		Map<String, Object> props = KafkaTestUtils.consumerProps("test12", "true", embeddedKafka);
		testSeekGuts(props, topic12, true);
	}

	@Test
	public void testSeekAutoCommitDefault() throws Exception {
		Map<String, Object> props = KafkaTestUtils.consumerProps("test15", "true", embeddedKafka);
		props.remove(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG); // test true by default
		testSeekGuts(props, topic15, true);
	}

	@Test
	public void testSeekBatch() throws Exception {
		logger.info("Start seek batch seek");
		Map<String, Object> props = KafkaTestUtils.consumerProps("test16", "true", embeddedKafka);
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(props);
		ContainerProperties containerProps = new ContainerProperties(topic16);
		final CountDownLatch registerLatch = new CountDownLatch(1);
		final CountDownLatch assignedLatch = new CountDownLatch(1);
		final CountDownLatch idleLatch = new CountDownLatch(1);
		class Listener implements BatchMessageListener<Integer, String>, ConsumerSeekAware {

			@Override
			public void onMessage(List<ConsumerRecord<Integer, String>> data) {
				// empty
			}

			@Override
			public void registerSeekCallback(ConsumerSeekCallback callback) {
				registerLatch.countDown();
			}

			@Override
			public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
				assignedLatch.countDown();
			}

			@Override
			public void onIdleContainer(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
				idleLatch.countDown();
			}

		}
		Listener messageListener = new Listener();
		containerProps.setMessageListener(messageListener);
		containerProps.setSyncCommits(true);
		containerProps.setAckOnError(false);
		containerProps.setIdleEventInterval(10L);
		KafkaMessageListenerContainer<Integer, String> container = new KafkaMessageListenerContainer<>(cf,
				containerProps);
		container.setBeanName("testBatchSeek");
		container.start();
		assertThat(registerLatch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(assignedLatch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(idleLatch.await(10, TimeUnit.SECONDS)).isTrue();
		container.stop();
	}

	private void testSeekGuts(Map<String, Object> props, String topic, boolean autoCommit) throws Exception {
		logger.info("Start seek " + topic);
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(props);
		ContainerProperties containerProps = new ContainerProperties(topic);
		final AtomicReference<CountDownLatch> latch = new AtomicReference<>(new CountDownLatch(6));
		final AtomicBoolean seekInitial = new AtomicBoolean();
		final CountDownLatch idleLatch = new CountDownLatch(1);
		class Listener implements MessageListener<Integer, String>, ConsumerSeekAware {

			private ConsumerSeekCallback callback;

			private Thread registerThread;

			private Thread messageThread;

			@Override
			public void onMessage(ConsumerRecord<Integer, String> data) {
				messageThread = Thread.currentThread();
				latch.get().countDown();
				if (latch.get().getCount() == 2 && !seekInitial.get()) {
					callback.seekToEnd(topic, 0);
					callback.seekToBeginning(topic, 0);
					callback.seek(topic, 0, 1);
					callback.seek(topic, 1, 1);
				}
			}

			@Override
			public void registerSeekCallback(ConsumerSeekCallback callback) {
				this.callback = callback;
				this.registerThread = Thread.currentThread();
			}

			@Override
			public void onPartitionsAssigned(Map<TopicPartition, Long> assignments,
					ConsumerSeekCallback callback) {
				if (seekInitial.get()) {
					for (Entry<TopicPartition, Long> assignment : assignments.entrySet()) {
						callback.seek(assignment.getKey().topic(), assignment.getKey().partition(),
								assignment.getValue() - 1);
					}
				}
			}

			@Override
			public void onIdleContainer(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
				for (Entry<TopicPartition, Long> assignment : assignments.entrySet()) {
					callback.seek(assignment.getKey().topic(), assignment.getKey().partition(),
							assignment.getValue() - 1);
				}
				idleLatch.countDown();
			}

		}
		Listener messageListener = new Listener();
		containerProps.setMessageListener(messageListener);
		containerProps.setSyncCommits(true);
		containerProps.setAckMode(AckMode.RECORD);
		containerProps.setAckOnError(false);
		containerProps.setIdleEventInterval(60000L);

		KafkaMessageListenerContainer<Integer, String> container = new KafkaMessageListenerContainer<>(cf,
				containerProps);
		container.setBeanName("testSeek" + topic);
		container.start();
		assertThat(KafkaTestUtils.getPropertyValue(container, "listenerConsumer.autoCommit", Boolean.class))
				.isEqualTo(autoCommit);
		Consumer<?, ?> consumer = spyOnConsumer(container);
		ContainerTestUtils.waitForAssignment(container, embeddedKafka.getPartitionsPerTopic());
		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		ProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf);
		template.setDefaultTopic(topic);
		template.sendDefault(0, 0, "foo");
		template.sendDefault(1, 0, "bar");
		template.sendDefault(0, 0, "baz");
		template.sendDefault(1, 0, "qux");
		template.flush();
		assertThat(latch.get().await(60, TimeUnit.SECONDS)).isTrue();
		container.stop();
		assertThat(messageListener.registerThread).isSameAs(messageListener.messageThread);

		// Now test initial seek of assigned partitions.
		latch.set(new CountDownLatch(2));
		seekInitial.set(true);
		container.start();
		assertThat(latch.get().await(60, TimeUnit.SECONDS)).isTrue();

		// Now seek on idle
		latch.set(new CountDownLatch(2));
		seekInitial.set(true);
		container.getContainerProperties().setIdleEventInterval(100L);
		final AtomicBoolean idleEventPublished = new AtomicBoolean();
		container.setApplicationEventPublisher(new ApplicationEventPublisher() {

			@Override
			public void publishEvent(Object event) {
				// NOSONAR
			}

			@Override
			public void publishEvent(ApplicationEvent event) {
				idleEventPublished.set(true);
			}

		});
		assertThat(idleLatch.await(60, TimeUnit.SECONDS));
		assertThat(idleEventPublished.get()).isTrue();
		assertThat(latch.get().await(60, TimeUnit.SECONDS)).isTrue();
		container.stop();
		@SuppressWarnings("unchecked")
		ArgumentCaptor<Collection<TopicPartition>> captor = ArgumentCaptor.forClass(Collection.class);
		verify(consumer).seekToBeginning(captor.capture());
		TopicPartition next = captor.getValue().iterator().next();
		assertThat(next.topic()).isEqualTo(topic);
		assertThat(next.partition()).isEqualTo(0);
		verify(consumer).seekToEnd(captor.capture());
		next = captor.getValue().iterator().next();
		assertThat(next.topic()).isEqualTo(topic);
		assertThat(next.partition()).isEqualTo(0);
		logger.info("Stop seek");
	}

	@Test
	public void testDefinedPartitions() throws Exception {
		this.logger.info("Start defined parts");
		Map<String, Object> props = KafkaTestUtils.consumerProps("test13", "false", embeddedKafka);
		TopicPartitionInitialOffset topic1Partition0 = new TopicPartitionInitialOffset(topic13, 0, 0L);

		CountDownLatch initialConsumersLatch = new CountDownLatch(2);

		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<Integer, String>(props) {

			@Override
			public Consumer<Integer, String> createConsumer(String groupId, String clientIdPrefix,
					String clientIdSuffix) {
				return new KafkaConsumer<Integer, String>(props) {

					@Override
					public ConsumerRecords<Integer, String> poll(Duration timeout) {
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
		CountDownLatch latch1 = new CountDownLatch(2);
		container1Props.setMessageListener((MessageListener<Integer, String>) message -> {
			logger.info("defined part: " + message);
			latch1.countDown();
		});
		CountDownLatch stubbingComplete1 = new CountDownLatch(1);
		KafkaMessageListenerContainer<Integer, String> container1 = spyOnContainer(
				new KafkaMessageListenerContainer<>(cf, container1Props), stubbingComplete1);
		container1.setBeanName("b1");
		container1.start();

		CountDownLatch stopLatch1 = new CountDownLatch(1);

		willAnswer(invocation -> {

			try {
				return invocation.callRealMethod();
			}
			finally {
				stopLatch1.countDown();
			}

		}).given(spyOnConsumer(container1))
				.commitSync(anyMap());
		stubbingComplete1.countDown();

		TopicPartitionInitialOffset topic1Partition1 = new TopicPartitionInitialOffset(topic13, 1, 0L);
		ContainerProperties container2Props = new ContainerProperties(topic1Partition1);
		CountDownLatch latch2 = new CountDownLatch(2);
		container2Props.setMessageListener((MessageListener<Integer, String>) message -> {
			logger.info("defined part: " + message);
			latch2.countDown();
		});
		CountDownLatch stubbingComplete2 = new CountDownLatch(1);
		KafkaMessageListenerContainer<Integer, String> container2 = spyOnContainer(
				new KafkaMessageListenerContainer<>(cf, container2Props), stubbingComplete2);
		container2.setBeanName("b2");
		container2.start();

		CountDownLatch stopLatch2 = new CountDownLatch(1);

		willAnswer(invocation -> {

			try {
				return invocation.callRealMethod();
			}
			finally {
				stopLatch2.countDown();
			}

		}).given(spyOnConsumer(container2))
				.commitSync(anyMap());
		stubbingComplete2.countDown();

		assertThat(initialConsumersLatch.await(20, TimeUnit.SECONDS)).isTrue();

		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		ProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf);
		template.setDefaultTopic(topic13);
		template.sendDefault(0, 0, "foo");
		template.sendDefault(1, 2, "bar");
		template.sendDefault(0, 0, "baz");
		template.sendDefault(1, 2, "qux");
		template.flush();

		assertThat(latch1.await(60, TimeUnit.SECONDS)).isTrue();
		assertThat(latch2.await(60, TimeUnit.SECONDS)).isTrue();

		assertThat(stopLatch1.await(60, TimeUnit.SECONDS)).isTrue();
		container1.stop();
		assertThat(stopLatch2.await(60, TimeUnit.SECONDS)).isTrue();
		container2.stop();

		cf = new DefaultKafkaConsumerFactory<>(props);
		// reset earliest
		ContainerProperties container3Props = new ContainerProperties(topic1Partition0, topic1Partition1);

		CountDownLatch latch3 = new CountDownLatch(4);
		container3Props.setMessageListener((MessageListener<Integer, String>) message -> {
			logger.info("defined part e: " + message);
			latch3.countDown();
		});

		final CountDownLatch listenerConsumerAvailableLatch = new CountDownLatch(1);

		final CountDownLatch listenerConsumerStartLatch = new CountDownLatch(1);

		CountDownLatch stubbingComplete3 = new CountDownLatch(1);
		KafkaMessageListenerContainer<Integer, String> resettingContainer = spyOnContainer(
				new KafkaMessageListenerContainer<Integer, String>(cf, container3Props), stubbingComplete3);
		stubSetRunning(listenerConsumerAvailableLatch, listenerConsumerStartLatch, resettingContainer);
		resettingContainer.setBeanName("b3");

		Executors.newSingleThreadExecutor().submit(resettingContainer::start);

		CountDownLatch stopLatch3 = new CountDownLatch(1);

		assertThat(listenerConsumerAvailableLatch.await(60, TimeUnit.SECONDS)).isTrue();

		willAnswer(invocation -> {

			try {
				return invocation.callRealMethod();
			}
			finally {
				stopLatch3.countDown();
			}

		}).given(spyOnConsumer(resettingContainer))
				.commitSync(anyMap());
		stubbingComplete3.countDown();

		listenerConsumerStartLatch.countDown();

		assertThat(latch3.await(60, TimeUnit.SECONDS)).isTrue();

		assertThat(stopLatch3.await(60, TimeUnit.SECONDS)).isTrue();
		resettingContainer.stop();
		assertThat(latch3.getCount()).isEqualTo(0L);

		cf = new DefaultKafkaConsumerFactory<>(props);
		// reset beginning for part 0, minus one for part 1
		topic1Partition0 = new TopicPartitionInitialOffset(topic13, 0, -1000L);
		topic1Partition1 = new TopicPartitionInitialOffset(topic13, 1, -1L);
		ContainerProperties container4Props = new ContainerProperties(topic1Partition0, topic1Partition1);

		CountDownLatch latch4 = new CountDownLatch(3);
		AtomicReference<String> receivedMessage = new AtomicReference<>();
		container4Props.setMessageListener((MessageListener<Integer, String>) message -> {
			logger.info("defined part 0, -1: " + message);
			receivedMessage.set(message.value());
			latch4.countDown();
		});

		CountDownLatch stubbingComplete4 = new CountDownLatch(1);
		resettingContainer = spyOnContainer(new KafkaMessageListenerContainer<>(cf, container4Props),
				stubbingComplete4);
		resettingContainer.setBeanName("b4");

		resettingContainer.start();

		CountDownLatch stopLatch4 = new CountDownLatch(1);

		willAnswer(invocation -> {

			try {
				return invocation.callRealMethod();
			}
			finally {
				stopLatch4.countDown();
			}

		}).given(spyOnConsumer(resettingContainer))
				.commitSync(anyMap());
		stubbingComplete4.countDown();

		assertThat(latch4.await(60, TimeUnit.SECONDS)).isTrue();

		assertThat(stopLatch4.await(60, TimeUnit.SECONDS)).isTrue();
		resettingContainer.stop();
		assertThat(receivedMessage.get()).isIn("baz", "qux");
		assertThat(latch4.getCount()).isEqualTo(0L);

		// reset plus one
		template.sendDefault(0, 0, "FOO");
		template.sendDefault(1, 2, "BAR");
		template.flush();

		topic1Partition0 = new TopicPartitionInitialOffset(topic13, 0, 1L);
		topic1Partition1 = new TopicPartitionInitialOffset(topic13, 1, 1L);
		ContainerProperties container5Props = new ContainerProperties(topic1Partition0, topic1Partition1);

		final CountDownLatch latch5 = new CountDownLatch(4);
		final List<String> messages = new ArrayList<>();
		container5Props.setMessageListener((MessageListener<Integer, String>) message -> {
			logger.info("defined part 1: " + message);
			messages.add(message.value());
			latch5.countDown();
		});

		CountDownLatch stubbingComplete5 = new CountDownLatch(1);
		resettingContainer = spyOnContainer(new KafkaMessageListenerContainer<>(cf, container5Props),
				stubbingComplete5);
		resettingContainer.setBeanName("b5");
		resettingContainer.start();

		CountDownLatch stopLatch5 = new CountDownLatch(1);

		willAnswer(invocation -> {

			try {
				return invocation.callRealMethod();
			}
			finally {
				stopLatch5.countDown();
			}

		}).given(spyOnConsumer(resettingContainer))
				.commitSync(anyMap());
		stubbingComplete5.countDown();

		assertThat(latch5.await(60, TimeUnit.SECONDS)).isTrue();

		assertThat(stopLatch5.await(60, TimeUnit.SECONDS)).isTrue();
		resettingContainer.stop();
		assertThat(messages).contains("baz", "qux", "FOO", "BAR");

		this.logger.info("+++++++++++++++++++++ Start relative reset");

		template.sendDefault(0, 0, "BAZ");
		template.sendDefault(1, 2, "QUX");
		template.sendDefault(0, 0, "FIZ");
		template.sendDefault(1, 2, "BUZ");
		template.flush();

		topic1Partition0 = new TopicPartitionInitialOffset(topic13, 0, 1L, true);
		topic1Partition1 = new TopicPartitionInitialOffset(topic13, 1, -1L, true);
		ContainerProperties container6Props = new ContainerProperties(topic1Partition0, topic1Partition1);

		final CountDownLatch latch6 = new CountDownLatch(4);
		final List<String> messages6 = new ArrayList<>();
		container6Props.setMessageListener((MessageListener<Integer, String>) message -> {
			logger.info("defined part relative: " + message);
			messages6.add(message.value());
			latch6.countDown();
		});

		CountDownLatch stubbingComplete6 = new CountDownLatch(1);
		resettingContainer = spyOnContainer(new KafkaMessageListenerContainer<>(cf, container6Props),
				stubbingComplete6);
		resettingContainer.setBeanName("b6");
		resettingContainer.start();

		CountDownLatch stopLatch6 = new CountDownLatch(1);

		willAnswer(invocation -> {

			try {
				return invocation.callRealMethod();
			}
			finally {
				stopLatch6.countDown();
			}

		}).given(spyOnConsumer(resettingContainer))
				.commitSync(anyMap());
		stubbingComplete6.countDown();

		assertThat(latch6.await(60, TimeUnit.SECONDS)).isTrue();

		assertThat(stopLatch6.await(60, TimeUnit.SECONDS)).isTrue();
		resettingContainer.stop();
		assertThat(messages6).hasSize(4);
		assertThat(messages6).contains("FIZ", "BAR", "QUX", "BUZ");

		this.logger.info("Stop auto parts");
	}

	private void stubSetRunning(final CountDownLatch listenerConsumerAvailableLatch,
			final CountDownLatch listenerConsumerStartLatch,
			KafkaMessageListenerContainer<Integer, String> resettingContainer) {
		willAnswer(invocation -> {
			listenerConsumerAvailableLatch.countDown();
			try {
				assertThat(listenerConsumerStartLatch.await(10, TimeUnit.SECONDS)).isTrue();
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new IllegalStateException(e);
			}
			return invocation.callRealMethod();
		}).given(resettingContainer).setRunning(true);
	}

	@Test
	public void testManualAckRebalance() throws Exception {
		logger.info("Start manual ack rebalance");
		Map<String, Object> props = KafkaTestUtils.consumerProps("test14", "false", embeddedKafka);
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(props);
		ContainerProperties containerProps = new ContainerProperties(topic14);
		final List<AtomicInteger> counts = new ArrayList<>();
		counts.add(new AtomicInteger());
		counts.add(new AtomicInteger());
		final Acknowledgment[] pendingAcks = new Acknowledgment[2];
		containerProps.setMessageListener((AcknowledgingMessageListener<Integer, String>) (message, ack) -> {
			logger.info("manual ack: " + message);
			if (counts.get(message.partition()).incrementAndGet() < 2) {
				ack.acknowledge();
			}
			else {
				pendingAcks[message.partition()] = ack;
			}
		});
		containerProps.setSyncCommits(true);
		containerProps.setAckMode(AckMode.MANUAL_IMMEDIATE);
		final CountDownLatch rebalanceLatch = new CountDownLatch(2);
		containerProps.setConsumerRebalanceListener(new ConsumerRebalanceListener() {

			@Override
			public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
				logger.info("manual ack: revoked " + partitions);
				partitions.forEach(p -> {
					if (pendingAcks[p.partition()] != null) {
						pendingAcks[p.partition()].acknowledge();
						pendingAcks[p.partition()] = null;
					}
				});
			}

			@Override
			public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
				logger.info("manual ack: assigned " + partitions);
				rebalanceLatch.countDown();
			}
		});

		CountDownLatch stubbingComplete1 = new CountDownLatch(1);
		KafkaMessageListenerContainer<Integer, String> container1 = spyOnContainer(
				new KafkaMessageListenerContainer<>(cf, containerProps), stubbingComplete1);
		container1.setBeanName("testAckRebalance");
		container1.start();
		Consumer<?, ?> containerConsumer = spyOnConsumer(container1);
		final CountDownLatch commitLatch = new CountDownLatch(2);
		willAnswer(invocation -> {

			Map<TopicPartition, OffsetAndMetadata> map = invocation.getArgument(0);
			try {
				return invocation.callRealMethod();
			}
			finally {
				for (Entry<TopicPartition, OffsetAndMetadata> entry : map.entrySet()) {
					if (entry.getValue().offset() == 1) {
						commitLatch.countDown();
					}
				}
			}

		}).given(containerConsumer)
				.commitSync(anyMap());
		stubbingComplete1.countDown();
		ContainerTestUtils.waitForAssignment(container1, embeddedKafka.getPartitionsPerTopic());
		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		ProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf);
		template.setDefaultTopic(topic14);
		template.sendDefault(0, 0, "foo");
		template.sendDefault(1, 0, "baz");
		template.sendDefault(0, 0, "bar");
		template.sendDefault(1, 0, "qux");
		template.flush();
		assertThat(commitLatch.await(60, TimeUnit.SECONDS)).isTrue();
		KafkaMessageListenerContainer<Integer, String> container2 = new KafkaMessageListenerContainer<>(cf,
				containerProps);
		container2.setBeanName("testAckRebalance2");
		container2.start();
		assertThat(rebalanceLatch.await(60, TimeUnit.SECONDS)).isTrue();
		container1.stop();
		container2.stop();
		Consumer<Integer, String> consumer = cf.createConsumer();
		consumer.assign(Arrays.asList(new TopicPartition(topic14, 0), new TopicPartition(topic14, 1)));
		assertThat(consumer.position(new TopicPartition(topic14, 0))).isEqualTo(2);
		assertThat(consumer.position(new TopicPartition(topic14, 1))).isEqualTo(2);
		consumer.close();
		logger.info("Stop manual ack rebalance");
	}

	@Test
	public void testJsonSerDeConfiguredType() throws Exception {
		this.logger.info("Start JSON1");
		Map<String, Object> props = KafkaTestUtils.consumerProps("testJson", "false", embeddedKafka);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
		props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, Foo.class);
		DefaultKafkaConsumerFactory<Integer, Foo> cf = new DefaultKafkaConsumerFactory<>(props);
		ContainerProperties containerProps = new ContainerProperties(topic1);

		final CountDownLatch latch = new CountDownLatch(1);
		final AtomicReference<ConsumerRecord<?, ?>> received = new AtomicReference<>();
		containerProps.setMessageListener((MessageListener<Integer, Foo>) record -> {
			KafkaMessageListenerContainerTests.this.logger.info("json: " + record);
			received.set(record);
			latch.countDown();
		});

		KafkaMessageListenerContainer<Integer, Foo> container =
				new KafkaMessageListenerContainer<>(cf, containerProps);
		container.setBeanName("testJson1");
		container.start();

		ContainerTestUtils.waitForAssignment(container, embeddedKafka.getPartitionsPerTopic());

		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		senderProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
		senderProps.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, false);
		ProducerFactory<Integer, Foo> pf = new DefaultKafkaProducerFactory<>(senderProps);
		KafkaTemplate<Integer, Foo> template = new KafkaTemplate<>(pf);
		template.setDefaultTopic(topic1);
		template.sendDefault(0, new Foo("bar"));
		template.flush();
		assertThat(latch.await(60, TimeUnit.SECONDS)).isTrue();
		assertThat(received.get().value()).isInstanceOf(Foo.class);
		container.stop();
		this.logger.info("Stop JSON1");
	}

	@Test
	public void testJsonSerDeHeaderSimpleType() throws Exception {
		this.logger.info("Start JSON2");
		Map<String, Object> props = KafkaTestUtils.consumerProps("testJson", "false", embeddedKafka);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
		props.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
		DefaultKafkaConsumerFactory<Integer, Foo> cf = new DefaultKafkaConsumerFactory<>(props);
		ContainerProperties containerProps = new ContainerProperties(topic2);

		final CountDownLatch latch = new CountDownLatch(1);
		final AtomicReference<ConsumerRecord<?, ?>> received = new AtomicReference<>();
		containerProps.setMessageListener((MessageListener<Integer, Foo>) record -> {
			KafkaMessageListenerContainerTests.this.logger.info("json: " + record);
			received.set(record);
			latch.countDown();
		});

		KafkaMessageListenerContainer<Integer, Foo> container =
				new KafkaMessageListenerContainer<>(cf, containerProps);
		container.setBeanName("testJson2");
		container.start();

		ContainerTestUtils.waitForAssignment(container, embeddedKafka.getPartitionsPerTopic());

		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		senderProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
		ProducerFactory<Integer, Foo> pf = new DefaultKafkaProducerFactory<>(senderProps);
		KafkaTemplate<Integer, Foo> template = new KafkaTemplate<>(pf);
		template.setDefaultTopic(topic2);
		template.sendDefault(0, new Foo("bar"));
		template.flush();
		assertThat(latch.await(60, TimeUnit.SECONDS)).isTrue();
		assertThat(received.get().value()).isInstanceOf(Foo.class);
		container.stop();
		this.logger.info("Stop JSON2");
		assertThat(received.get().headers().iterator().hasNext()).isFalse();
	}

	@Test
	public void testJsonSerDeTypeMappings() throws Exception {
		this.logger.info("Start JSON3");
		Map<String, Object> props = KafkaTestUtils.consumerProps("testJson", "false", embeddedKafka);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
		props.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
		props.put(JsonDeserializer.TYPE_MAPPINGS, "foo:" + Foo1.class.getName() + " , bar:" + Bar1.class.getName());
		DefaultKafkaConsumerFactory<Integer, Foo1> cf = new DefaultKafkaConsumerFactory<>(props);
		ContainerProperties containerProps = new ContainerProperties(topic20);

		final CountDownLatch latch = new CountDownLatch(2);
		final List<ConsumerRecord<Integer, Foo1>> received = new ArrayList<>();
		containerProps.setMessageListener((MessageListener<Integer, Foo1>) record -> {
			KafkaMessageListenerContainerTests.this.logger.info("json: " + record);
			received.add(record);
			latch.countDown();
		});

		KafkaMessageListenerContainer<Integer, Foo1> container =
				new KafkaMessageListenerContainer<>(cf, containerProps);
		container.setBeanName("testJson3");
		container.start();

		ContainerTestUtils.waitForAssignment(container, embeddedKafka.getPartitionsPerTopic());

		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		senderProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
		senderProps.put(JsonSerializer.TYPE_MAPPINGS, "foo:" + Foo.class.getName() + ",bar:" + Bar.class.getName());
		ProducerFactory<Integer, Foo> pf = new DefaultKafkaProducerFactory<>(senderProps);
		KafkaTemplate<Integer, Foo> template = new KafkaTemplate<>(pf);
		template.setDefaultTopic(topic20);
		template.sendDefault(0, new Foo("bar"));
		template.sendDefault(0, new Bar("baz"));
		assertThat(latch.await(60, TimeUnit.SECONDS)).isTrue();
		assertThat(received.get(0).value().getClass()).isEqualTo(Foo1.class);
		assertThat(received.get(1).value().getClass()).isEqualTo(Bar1.class);
		container.stop();
		this.logger.info("Stop JSON3");
	}

	@Test
	public void testJsonSerDeIgnoreTypeHeadersInbound() throws Exception {
		this.logger.info("Start JSON4");
		Map<String, Object> props = KafkaTestUtils.consumerProps("testJson", "false", embeddedKafka);

		ErrorHandlingDeserializer2<Foo1> errorHandlingDeserializer =
				new ErrorHandlingDeserializer2<>(new JsonDeserializer<>(Foo1.class, false));

		DefaultKafkaConsumerFactory<Integer, Foo1> cf = new DefaultKafkaConsumerFactory<>(props,
				new IntegerDeserializer(), errorHandlingDeserializer);
		ContainerProperties containerProps = new ContainerProperties(topic21);

		final CountDownLatch latch = new CountDownLatch(1);
		final List<ConsumerRecord<Integer, Foo1>> received = new ArrayList<>();
		containerProps.setMessageListener((MessageListener<Integer, Foo1>) record -> {
			KafkaMessageListenerContainerTests.this.logger.info("json: " + record);
			received.add(record);
			latch.countDown();
		});

		KafkaMessageListenerContainer<Integer, Foo1> container =
				new KafkaMessageListenerContainer<>(cf, containerProps);
		container.setBeanName("testJson4");
		container.start();

		ContainerTestUtils.waitForAssignment(container, embeddedKafka.getPartitionsPerTopic());

		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		senderProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
		ProducerFactory<Integer, Foo> pf = new DefaultKafkaProducerFactory<>(senderProps);
		KafkaTemplate<Integer, Foo> template = new KafkaTemplate<>(pf);
		template.setDefaultTopic(topic21);
		template.sendDefault(0, new Foo("bar"));
		assertThat(latch.await(60, TimeUnit.SECONDS)).isTrue();
		assertThat(received.get(0).value().getClass()).isEqualTo(Foo1.class);
		container.stop();
		this.logger.info("Stop JSON4");
	}

	@Test
	public void testRebalanceAfterFailedRecord() throws Exception {
		logger.info("Start rebalance after failed record");
		Map<String, Object> props = KafkaTestUtils.consumerProps("test18", "false", embeddedKafka);
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(props);
		ContainerProperties containerProps = new ContainerProperties(topic18);
		final List<AtomicInteger> counts = new ArrayList<>();
		counts.add(new AtomicInteger());
		counts.add(new AtomicInteger());
		containerProps.setMessageListener(new MessageListener<Integer, String>() {

			@Override
			public void onMessage(ConsumerRecord<Integer, String> message) {
				// The 1st message per partition fails
				if (counts.get(message.partition()).incrementAndGet() < 2) {
					throw new RuntimeException("Failure wile processing message");
				}
			}
		});
		containerProps.setSyncCommits(true);
		containerProps.setAckMode(AckMode.RECORD);
		final CountDownLatch rebalanceLatch = new CountDownLatch(2);
		containerProps.setConsumerRebalanceListener(new ConsumerRebalanceListener() {

			@Override
			public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
			}

			@Override
			public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
				logger.info("manual ack: assigned " + partitions);
				rebalanceLatch.countDown();
			}
		});

		CountDownLatch stubbingComplete1 = new CountDownLatch(1);
		KafkaMessageListenerContainer<Integer, String> container1 =
				spyOnContainer(new KafkaMessageListenerContainer<>(cf, containerProps), stubbingComplete1);
		container1.setBeanName("testRebalanceAfterFailedRecord");
		container1.start();
		Consumer<?, ?> containerConsumer = spyOnConsumer(container1);
		final CountDownLatch commitLatch = new CountDownLatch(2);
		willAnswer(invocation -> {

			Map<TopicPartition, OffsetAndMetadata> map = invocation.getArgument(0);
			try {
				return invocation.callRealMethod();
			}
			finally {
				for (Entry<TopicPartition, OffsetAndMetadata> entry : map.entrySet()) {
					// Decrement when the last (successful) has been committed
					if (entry.getValue().offset() == 2) {
						commitLatch.countDown();
					}
				}
			}

		}).given(containerConsumer).commitSync(anyMap());
		stubbingComplete1.countDown();
		ContainerTestUtils.waitForAssignment(container1, embeddedKafka.getPartitionsPerTopic());

		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		ProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf);
		template.setDefaultTopic(topic18);
		template.sendDefault(0, 0, "foo");
		template.sendDefault(1, 0, "baz");
		template.sendDefault(0, 0, "bar");
		template.sendDefault(1, 0, "qux");
		template.flush();

		// Wait until both partitions have committed offset 2 (i.e. the last message)
		assertThat(commitLatch.await(30, TimeUnit.SECONDS)).isTrue();

		// Start a 2nd consumer, triggering a rebalance
		KafkaMessageListenerContainer<Integer, String> container2 =
				new KafkaMessageListenerContainer<>(cf, containerProps);
		container2.setBeanName("testRebalanceAfterFailedRecord2");
		container2.start();
		// Wait until both consumers have finished rebalancing
		assertThat(rebalanceLatch.await(60, TimeUnit.SECONDS)).isTrue();

		// Stop both consumers
		container1.stop();
		container2.stop();
		Consumer<Integer, String> consumer = cf.createConsumer();
		consumer.assign(Arrays.asList(new TopicPartition(topic18, 0), new TopicPartition(topic18, 1)));

		// Verify that offset of both partitions is the highest committed offset
		assertThat(consumer.position(new TopicPartition(topic18, 0))).isEqualTo(2);
		assertThat(consumer.position(new TopicPartition(topic18, 1))).isEqualTo(2);
		consumer.close();
		logger.info("Stop rebalance after failed record");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testPauseResume() throws Exception {
		ConsumerFactory<Integer, String> cf = mock(ConsumerFactory.class);
		Consumer<Integer, String> consumer = mock(Consumer.class);
		given(cf.createConsumer(eq("grp"), eq("clientId"), isNull())).willReturn(consumer);
		final Map<TopicPartition, List<ConsumerRecord<Integer, String>>> records = new HashMap<>();
		records.put(new TopicPartition("foo", 0), Arrays.asList(
				new ConsumerRecord<>("foo", 0, 0L, 1, "foo"),
				new ConsumerRecord<>("foo", 0, 1L, 1, "bar")));
		ConsumerRecords<Integer, String> consumerRecords = new ConsumerRecords<>(records);
		ConsumerRecords<Integer, String> emptyRecords = new ConsumerRecords<>(Collections.emptyMap());
		AtomicBoolean first = new AtomicBoolean(true);
		given(consumer.poll(any(Duration.class))).willAnswer(i -> {
			Thread.sleep(50);
			return first.getAndSet(false) ? consumerRecords : emptyRecords;
		});
		final CountDownLatch commitLatch = new CountDownLatch(2);
		willAnswer(i -> {
			commitLatch.countDown();
			return null;
		}).given(consumer).commitSync(any(Map.class));
		given(consumer.assignment()).willReturn(records.keySet());
		final CountDownLatch pauseLatch = new CountDownLatch(2);
		willAnswer(i -> {
			pauseLatch.countDown();
			return null;
		}).given(consumer).pause(records.keySet());
		given(consumer.paused()).willReturn(records.keySet());
		final CountDownLatch resumeLatch = new CountDownLatch(2);
		willAnswer(i -> {
			resumeLatch.countDown();
			return null;
		}).given(consumer).resume(records.keySet());
		TopicPartitionInitialOffset[] topicPartition = new TopicPartitionInitialOffset[] {
				new TopicPartitionInitialOffset("foo", 0) };
		ContainerProperties containerProps = new ContainerProperties(topicPartition);
		containerProps.setGroupId("grp");
		containerProps.setAckMode(AckMode.RECORD);
		containerProps.setClientId("clientId");
		containerProps.setIdleEventInterval(100L);
		containerProps.setMessageListener((MessageListener) r -> { });
		KafkaMessageListenerContainer<Integer, String> container =
				new KafkaMessageListenerContainer<>(cf, containerProps);
		CountDownLatch stopLatch = new CountDownLatch(1);
		container.setApplicationEventPublisher(e -> {
			if (e instanceof ConsumerPausedEvent) {
				pauseLatch.countDown();
			}
			else if (e instanceof ConsumerResumedEvent) {
				resumeLatch.countDown();
			}
			else if (e instanceof ConsumerStoppedEvent) {
				stopLatch.countDown();
			}
		});
		container.start();
		assertThat(commitLatch.await(10, TimeUnit.SECONDS)).isTrue();
		verify(consumer, times(2)).commitSync(any(Map.class));
		container.pause();
		assertThat(pauseLatch.await(10, TimeUnit.SECONDS)).isTrue();
		container.resume();
		assertThat(resumeLatch.await(10, TimeUnit.SECONDS)).isTrue();
		container.stop();
		assertThat(stopLatch.await(10, TimeUnit.SECONDS)).isTrue();
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testInitialSeek() throws Exception {
		ConsumerFactory<Integer, String> cf = mock(ConsumerFactory.class);
		Consumer<Integer, String> consumer = mock(Consumer.class);
		given(cf.createConsumer(eq("grp"), eq("clientId"), isNull())).willReturn(consumer);
		ConsumerRecords<Integer, String> emptyRecords = new ConsumerRecords<>(Collections.emptyMap());
		final CountDownLatch latch = new CountDownLatch(1);
		given(consumer.poll(any(Duration.class))).willAnswer(i -> {
			latch.countDown();
			Thread.sleep(50);
			return emptyRecords;
		});
		TopicPartitionInitialOffset[] topicPartition = new TopicPartitionInitialOffset[] {
				new TopicPartitionInitialOffset("foo", 0, SeekPosition.BEGINNING),
				new TopicPartitionInitialOffset("foo", 1, SeekPosition.END),
				new TopicPartitionInitialOffset("foo", 2, 0L),
				new TopicPartitionInitialOffset("foo", 3, Long.MAX_VALUE),
				new TopicPartitionInitialOffset("foo", 4, SeekPosition.BEGINNING),
				new TopicPartitionInitialOffset("foo", 5, SeekPosition.END),
		};
		ContainerProperties containerProps = new ContainerProperties(topicPartition);
		containerProps.setGroupId("grp");
		containerProps.setAckMode(AckMode.RECORD);
		containerProps.setClientId("clientId");
		containerProps.setMessageListener((MessageListener) r -> { });
		KafkaMessageListenerContainer<Integer, String> container =
				new KafkaMessageListenerContainer<>(cf, containerProps);
		container.start();
		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		ArgumentCaptor<Collection<TopicPartition>> captor = ArgumentCaptor.forClass(List.class);
		verify(consumer).seekToBeginning(captor.capture());
		assertThat(captor.getValue())
				.isEqualTo(new HashSet<>(Arrays.asList(new TopicPartition("foo", 0), new TopicPartition("foo", 4))));
		verify(consumer).seekToEnd(captor.capture());
		assertThat(captor.getValue())
				.isEqualTo(new HashSet<>(Arrays.asList(new TopicPartition("foo", 1), new TopicPartition("foo", 5))));
		verify(consumer).seek(new TopicPartition("foo", 2), 0L);
		verify(consumer).seek(new TopicPartition("foo", 3), Long.MAX_VALUE);
		container.stop();
	}

	@Test
	public void testExceptionWhenCommitAfterRebalance() throws Exception {
		final CountDownLatch rebalanceLatch = new CountDownLatch(2);
		final CountDownLatch consumeLatch = new CountDownLatch(7);

		Map<String, Object> props = KafkaTestUtils.consumerProps("test19", "false", embeddedKafka);
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
		props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 15000);
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(props);
		ContainerProperties containerProps = new ContainerProperties(topic19);
		containerProps.setMessageListener((MessageListener<Integer, String>) messages -> {
			logger.info("listener: " + messages);
			consumeLatch.countDown();
			try {
				Thread.sleep(3000);
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		});
		containerProps.setSyncCommits(true);
		containerProps.setAckMode(AckMode.BATCH);
		containerProps.setPollTimeout(100);
		containerProps.setAckOnError(false);

		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		ProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf);
		template.setDefaultTopic(topic19);

		containerProps.setConsumerRebalanceListener(new ConsumerRebalanceListener() {

			@Override
			public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
			}

			@Override
			public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
				logger.info("rebalance occurred.");
				rebalanceLatch.countDown();
			}
		});

		KafkaMessageListenerContainer<Integer, String> container =
				new KafkaMessageListenerContainer<>(cf, containerProps);
		container.setBeanName("testContainerException");
		container.setErrorHandler(new SeekToCurrentErrorHandler());
		container.start();
		ContainerTestUtils.waitForAssignment(container, embeddedKafka.getPartitionsPerTopic());
		container.pause();

		for (int i = 0; i < 6; i++) {
			template.sendDefault(0, 0, "a");
		}
		template.flush();

		container.resume();
		// should be rebalanced and consume again
		assertThat(rebalanceLatch.await(60, TimeUnit.SECONDS)).isTrue();
		assertThat(consumeLatch.await(60, TimeUnit.SECONDS)).isTrue();
		container.stop();
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testAckModeCount() throws Exception {
		ConsumerFactory<Integer, String> cf = mock(ConsumerFactory.class);
		Consumer<Integer, String> consumer = mock(Consumer.class);
		given(cf.createConsumer(eq("grp"), eq("clientId"), isNull())).willReturn(consumer);
		TopicPartition topicPartition = new TopicPartition("foo", 0);
		final Map<TopicPartition, List<ConsumerRecord<Integer, String>>> records1 = new HashMap<>();
		records1.put(topicPartition, Arrays.asList(
				new ConsumerRecord<>("foo", 0, 0L, 1, "foo"),
				new ConsumerRecord<>("foo", 0, 1L, 1, "bar")));
		final Map<TopicPartition, List<ConsumerRecord<Integer, String>>> records2 = new HashMap<>();
		records2.put(topicPartition, Arrays.asList(
				new ConsumerRecord<>("foo", 0, 2L, 1, "baz"),
				new ConsumerRecord<>("foo", 0, 3L, 1, "qux"))); // commit (4 >= 3)
		final Map<TopicPartition, List<ConsumerRecord<Integer, String>>> records3 = new HashMap<>();
		records3.put(topicPartition, Arrays.asList(
				new ConsumerRecord<>("foo", 0, 4L, 1, "fiz"),
				new ConsumerRecord<>("foo", 0, 5L, 1, "buz"),
				new ConsumerRecord<>("foo", 0, 6L, 1, "bif"))); // commit (3 >= 3)
		ConsumerRecords<Integer, String> consumerRecords1 = new ConsumerRecords<>(records1);
		ConsumerRecords<Integer, String> consumerRecords2 = new ConsumerRecords<>(records2);
		ConsumerRecords<Integer, String> consumerRecords3 = new ConsumerRecords<>(records3);
		ConsumerRecords<Integer, String> emptyRecords = new ConsumerRecords<>(Collections.emptyMap());
		AtomicInteger which = new AtomicInteger();
		given(consumer.poll(any(Duration.class))).willAnswer(i -> {
			Thread.sleep(50);
			int recordsToUse = which.incrementAndGet();
			switch (recordsToUse) {
				case 1:
					return consumerRecords1;
				case 2:
					return consumerRecords2;
				case 3:
					return consumerRecords3;
				default:
					return emptyRecords;
			}
		});
		final CountDownLatch commitLatch = new CountDownLatch(2);
		willAnswer(i -> {
			commitLatch.countDown();
			return null;
		}).given(consumer).commitSync(any(Map.class));
		given(consumer.assignment()).willReturn(records1.keySet());
		TopicPartitionInitialOffset[] topicPartitionOffset = new TopicPartitionInitialOffset[] {
				new TopicPartitionInitialOffset("foo", 0) };
		ContainerProperties containerProps = new ContainerProperties(topicPartitionOffset);
		containerProps.setGroupId("grp");
		containerProps.setAckMode(AckMode.COUNT);
		containerProps.setAckCount(3);
		containerProps.setClientId("clientId");
		AtomicInteger recordCount = new AtomicInteger();
		containerProps.setMessageListener((MessageListener) r -> {
			recordCount.incrementAndGet();
		});
		KafkaMessageListenerContainer<Integer, String> container =
				new KafkaMessageListenerContainer<>(cf, containerProps);
		container.start();
		assertThat(commitLatch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(recordCount.get()).isEqualTo(7);
		verify(consumer).commitSync(Collections.singletonMap(topicPartition, new OffsetAndMetadata(4L)));
		verify(consumer).commitSync(Collections.singletonMap(topicPartition, new OffsetAndMetadata(7L)));
		container.stop();
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testCommitErrorHandlerCalled() throws Exception {
		ConsumerFactory<Integer, String> cf = mock(ConsumerFactory.class);
		Consumer<Integer, String> consumer = mock(Consumer.class);
		given(cf.createConsumer(eq("grp"), eq("clientId"), isNull())).willReturn(consumer);
		final Map<TopicPartition, List<ConsumerRecord<Integer, String>>> records = new HashMap<>();
		records.put(new TopicPartition("foo", 0), Arrays.asList(
				new ConsumerRecord<>("foo", 0, 0L, 1, "foo"),
				new ConsumerRecord<>("foo", 0, 1L, 1, "bar")));
		ConsumerRecords<Integer, String> consumerRecords = new ConsumerRecords<>(records);
		ConsumerRecords<Integer, String> emptyRecords = new ConsumerRecords<>(Collections.emptyMap());
		AtomicBoolean first = new AtomicBoolean(true);
		given(consumer.poll(any(Duration.class))).willAnswer(i -> {
			Thread.sleep(50);
			return first.getAndSet(false) ? consumerRecords : emptyRecords;
		});
		willAnswer(i -> {
			throw new RuntimeException("Commit failed");
		}).given(consumer).commitSync(any(Map.class));
		TopicPartitionInitialOffset[] topicPartition = new TopicPartitionInitialOffset[] {
				new TopicPartitionInitialOffset("foo", 0) };
		ContainerProperties containerProps = new ContainerProperties(topicPartition);
		containerProps.setGroupId("grp");
		containerProps.setClientId("clientId");
		containerProps.setIdleEventInterval(100L);
		containerProps.setMessageListener((MessageListener) r -> {
		});
		KafkaMessageListenerContainer<Integer, String> container =
				new KafkaMessageListenerContainer<>(cf, containerProps);
		final CountDownLatch ehl = new CountDownLatch(1);
		container.setErrorHandler((r, t) -> {
			ehl.countDown();
		});
		container.start();
		assertThat(ehl.await(10, TimeUnit.SECONDS)).isTrue();
		container.stop();
		containerProps.setMessageListener((BatchMessageListener) r -> {
		});
		container = new KafkaMessageListenerContainer<>(cf, containerProps);
		final CountDownLatch behl = new CountDownLatch(1);
		container.setBatchErrorHandler((r, t) -> {
			behl.countDown();
		});
		first.set(true);
		container.start();
		assertThat(behl.await(10, TimeUnit.SECONDS)).isTrue();
		container.stop();
	}

	private Consumer<?, ?> spyOnConsumer(KafkaMessageListenerContainer<Integer, String> container) {
		Consumer<?, ?> consumer = spy(
				KafkaTestUtils.getPropertyValue(container, "listenerConsumer.consumer", Consumer.class));
		new DirectFieldAccessor(KafkaTestUtils.getPropertyValue(container, "listenerConsumer"))
				.setPropertyValue("consumer", consumer);
		return consumer;
	}

	private KafkaMessageListenerContainer<Integer, String> spyOnContainer(KafkaMessageListenerContainer<Integer,
			String> container,
			final CountDownLatch stubbingComplete) {
		KafkaMessageListenerContainer<Integer, String> spy = spy(container);
		willAnswer(i -> {
			if (stubbingComplete.getCount() > 0 && Thread.currentThread().getName().endsWith("-C-1")) {
				try {
					stubbingComplete.await(10, TimeUnit.SECONDS);
				}
				catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
			}
			return i.callRealMethod();
		}).given(spy).isRunning();
		return spy;
	}

	@SuppressWarnings("serial")
	public static class FooEx extends RuntimeException {

	}

	public static class Foo {

		private String bar;

		public Foo() {
			super();
		}

		public Foo(String bar) {
			this.bar = bar;
		}

		public String getBar() {
			return this.bar;
		}

		public void setBar(String bar) {
			this.bar = bar;
		}

		@Override
		public String toString() {
			return "Foo [bar=" + this.bar + "]";
		}

	}

	public static class Foo1 {

		private String bar;

		public Foo1() {
			super();
		}

		public Foo1(String bar) {
			this.bar = bar;
		}

		public String getBar() {
			return this.bar;
		}

		public void setBar(String bar) {
			this.bar = bar;
		}

		@Override
		public String toString() {
			return "Foo1 [bar=" + this.bar + "]";
		}

	}

	public static class Bar extends Foo {

		private String baz;

		public Bar() {
			super();
		}

		public Bar(String baz) {
			this.baz = baz;
		}

		@SuppressWarnings("unused")
		private String getBaz() {
			return this.baz;
		}

		@SuppressWarnings("unused")
		private void setBaz(String baz) {
			this.baz = baz;
		}

		@Override
		public String toString() {
			return "Bar [baz=" + this.baz + "]";
		}

	}

	public static class Bar1 extends Foo1 {

		private String baz;

		public Bar1() {
			super();
		}

		public Bar1(String baz) {
			this.baz = baz;
		}

		@SuppressWarnings("unused")
		private String getBaz() {
			return this.baz;
		}

		@SuppressWarnings("unused")
		private void setBaz(String baz) {
			this.baz = baz;
		}

		@Override
		public String toString() {
			return "Bar1 [baz=" + this.baz + "]";
		}

	}

}
