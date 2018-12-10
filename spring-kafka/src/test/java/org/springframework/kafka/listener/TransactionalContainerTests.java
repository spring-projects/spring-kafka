/*
 * Copyright 2017-2018 the original author or authors.
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
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.BDDMockito.willReturn;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;

import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.core.ProducerFactoryUtils;
import org.springframework.kafka.event.ConsumerStoppedEvent;
import org.springframework.kafka.support.DefaultKafkaHeaderMapper;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.TopicPartitionInitialOffset;
import org.springframework.kafka.support.TransactionSupport;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.kafka.transaction.ChainedKafkaTransactionManager;
import org.springframework.kafka.transaction.KafkaTransactionManager;
import org.springframework.messaging.MessageHeaders;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;

import kafka.server.KafkaConfig;

/**
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @since 1.3
 *
 */
public class TransactionalContainerTests {

	private final Log logger = LogFactory.getLog(this.getClass());

	private static String topic1 = "txTopic1";

	private static String topic2 = "txTopic2";

	private static String topic3 = "txTopic3";

	private static String topic3DLT = "txTopic3.DLT";

	@ClassRule
	public static EmbeddedKafkaRule embeddedKafkaRule = new EmbeddedKafkaRule(1, true, topic1, topic2, topic3,
				topic3DLT)
			.brokerProperty(KafkaConfig.TransactionsTopicReplicationFactorProp(), "1")
			.brokerProperty(KafkaConfig.TransactionsTopicMinISRProp(), "1");

	private static EmbeddedKafkaBroker embeddedKafka = embeddedKafkaRule.getEmbeddedKafka();

	@Test
	public void testConsumeAndProduceTransactionKTM() throws Exception {
		testConsumeAndProduceTransactionGuts(false, false);
	}

	@Test
	public void testConsumeAndProduceTransactionKCTM() throws Exception {
		testConsumeAndProduceTransactionGuts(true, false);
	}

	@Test
	public void testConsumeAndProduceTransactionHandleError() throws Exception {
		testConsumeAndProduceTransactionGuts(false, true);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private void testConsumeAndProduceTransactionGuts(boolean chained, boolean handleError) throws Exception {
		Consumer consumer = mock(Consumer.class);
		final TopicPartition topicPartition = new TopicPartition("foo", 0);
		willAnswer(i -> {
			((ConsumerRebalanceListener) i.getArgument(1))
					.onPartitionsAssigned(Collections.singletonList(topicPartition));
			return null;
		}).given(consumer).subscribe(any(Collection.class), any(ConsumerRebalanceListener.class));
		ConsumerRecords records = new ConsumerRecords(Collections.singletonMap(topicPartition,
				Collections.singletonList(new ConsumerRecord<>("foo", 0, 0, "key", "value"))));
		final AtomicBoolean done = new AtomicBoolean();
		willAnswer(i -> {
			if (done.compareAndSet(false, true)) {
				return records;
			}
			else {
				Thread.sleep(500);
				return null;
			}
		}).given(consumer).poll(any(Duration.class));
		ConsumerFactory cf = mock(ConsumerFactory.class);
		willReturn(consumer).given(cf).createConsumer("group", "", null);
		Producer producer = mock(Producer.class);
		final CountDownLatch closeLatch = new CountDownLatch(2);
		willAnswer(i -> {
			closeLatch.countDown();
			return null;
		}).given(producer).close();
		ProducerFactory pf = mock(ProducerFactory.class);
		given(pf.transactionCapable()).willReturn(true);
		final List<String> transactionalIds = new ArrayList<>();
		willAnswer(i -> {
			transactionalIds.add(TransactionSupport.getTransactionIdSuffix());
			return producer;
		}).given(pf).createProducer();
		KafkaTransactionManager tm = new KafkaTransactionManager(pf);
		PlatformTransactionManager ptm = tm;
		if (chained) {
			ptm = new ChainedKafkaTransactionManager(new SomeOtherTransactionManager(), tm);
		}
		ContainerProperties props = new ContainerProperties("foo");
		props.setGroupId("group");
		props.setTransactionManager(ptm);
		final KafkaTemplate template = new KafkaTemplate(pf);
		props.setMessageListener((MessageListener) m -> {
			template.send("bar", "baz");
			if (handleError) {
				throw new RuntimeException("fail");
			}
		});
		KafkaMessageListenerContainer container = new KafkaMessageListenerContainer<>(cf, props);
		container.setBeanName("commit");
		if (handleError) {
			container.setErrorHandler((e, data) -> { });
		}
		container.start();
		assertThat(closeLatch.await(10, TimeUnit.SECONDS)).isTrue();
		InOrder inOrder = inOrder(producer);
		inOrder.verify(producer).beginTransaction();
		inOrder.verify(producer).sendOffsetsToTransaction(Collections.singletonMap(topicPartition,
				new OffsetAndMetadata(0)), "group");
		inOrder.verify(producer).commitTransaction();
		inOrder.verify(producer).close();
		inOrder.verify(producer).beginTransaction();
		ArgumentCaptor<ProducerRecord> captor = ArgumentCaptor.forClass(ProducerRecord.class);
		inOrder.verify(producer).send(captor.capture(), any(Callback.class));
		assertThat(captor.getValue()).isEqualTo(new ProducerRecord("bar", "baz"));
		inOrder.verify(producer).sendOffsetsToTransaction(Collections.singletonMap(topicPartition,
				new OffsetAndMetadata(1)), "group");
		inOrder.verify(producer).commitTransaction();
		inOrder.verify(producer).close();
		container.stop();
		verify(pf, times(2)).createProducer();
		verifyNoMoreInteractions(producer);
		assertThat(transactionalIds.get(0)).isEqualTo("group.foo.0");
		assertThat(transactionalIds.get(0)).isEqualTo("group.foo.0");
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testConsumeAndProduceTransactionRollback() throws Exception {
		Consumer consumer = mock(Consumer.class);
		final TopicPartition topicPartition0 = new TopicPartition("foo", 0);
		final TopicPartition topicPartition1 = new TopicPartition("foo", 1);
		Map<TopicPartition, List<ConsumerRecord<String, String>>> recordMap = new HashMap<>();
		recordMap.put(topicPartition0, Collections.singletonList(new ConsumerRecord<>("foo", 0, 0, "key", "value")));
		recordMap.put(topicPartition1, Collections.singletonList(new ConsumerRecord<>("foo", 1, 0, "key", "value")));
		ConsumerRecords records = new ConsumerRecords(recordMap);
		final AtomicBoolean done = new AtomicBoolean();
		willAnswer(i -> {
			if (done.compareAndSet(false, true)) {
				return records;
			}
			else {
				Thread.sleep(500);
				return null;
			}
		}).given(consumer).poll(any(Duration.class));
		final CountDownLatch seekLatch = new CountDownLatch(2);
		willAnswer(i -> {
			seekLatch.countDown();
			return null;
		}).given(consumer).seek(any(), anyLong());
		ConsumerFactory cf = mock(ConsumerFactory.class);
		willReturn(consumer).given(cf).createConsumer("group", "", null);
		Producer producer = mock(Producer.class);
		final CountDownLatch closeLatch = new CountDownLatch(1);
		willAnswer(i -> {
			closeLatch.countDown();
			return null;
		}).given(producer).close();
		ProducerFactory pf = mock(ProducerFactory.class);
		given(pf.transactionCapable()).willReturn(true);
		given(pf.createProducer()).willReturn(producer);
		KafkaTransactionManager tm = new KafkaTransactionManager(pf);
		ContainerProperties props = new ContainerProperties(new TopicPartitionInitialOffset("foo", 0),
				new TopicPartitionInitialOffset("foo", 1));
		props.setGroupId("group");
		props.setTransactionManager(tm);
		final KafkaTemplate template = new KafkaTemplate(pf);
		props.setMessageListener((MessageListener) m -> {
			template.send("bar", "baz");
			throw new RuntimeException("fail");
		});
		KafkaMessageListenerContainer container = new KafkaMessageListenerContainer<>(cf, props);
		container.setBeanName("rollback");
		container.start();
		assertThat(closeLatch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(seekLatch.await(10, TimeUnit.SECONDS)).isTrue();
		InOrder inOrder = inOrder(producer);
		inOrder.verify(producer).beginTransaction();
		ArgumentCaptor<ProducerRecord> captor = ArgumentCaptor.forClass(ProducerRecord.class);
		verify(producer).send(captor.capture(), any(Callback.class));
		assertThat(captor.getValue()).isEqualTo(new ProducerRecord("bar", "baz"));
		inOrder.verify(producer, never()).sendOffsetsToTransaction(anyMap(), anyString());
		inOrder.verify(producer, never()).commitTransaction();
		inOrder.verify(producer).abortTransaction();
		inOrder.verify(producer).close();
		verify(consumer).seek(topicPartition0, 0);
		verify(consumer).seek(topicPartition1, 0);
		verify(consumer, never()).commitSync(anyMap());
		container.stop();
		verify(pf, times(1)).createProducer();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testConsumeAndProduceTransactionRollbackBatch() throws Exception {
		Consumer consumer = mock(Consumer.class);
		final TopicPartition topicPartition0 = new TopicPartition("foo", 0);
		final TopicPartition topicPartition1 = new TopicPartition("foo", 1);
		Map<TopicPartition, List<ConsumerRecord<String, String>>> recordMap = new HashMap<>();
		recordMap.put(topicPartition0, Collections.singletonList(new ConsumerRecord<>("foo", 0, 0, "key", "value")));
		recordMap.put(topicPartition1, Collections.singletonList(new ConsumerRecord<>("foo", 1, 0, "key", "value")));
		ConsumerRecords records = new ConsumerRecords(recordMap);
		final AtomicBoolean done = new AtomicBoolean();
		willAnswer(i -> {
			if (done.compareAndSet(false, true)) {
				return records;
			}
			else {
				Thread.sleep(500);
				return null;
			}
		}).given(consumer).poll(any(Duration.class));
		final CountDownLatch seekLatch = new CountDownLatch(2);
		willAnswer(i -> {
			seekLatch.countDown();
			return null;
		}).given(consumer).seek(any(), anyLong());
		ConsumerFactory cf = mock(ConsumerFactory.class);
		willReturn(consumer).given(cf).createConsumer("group", "", null);
		Producer producer = mock(Producer.class);
		final CountDownLatch closeLatch = new CountDownLatch(1);
		willAnswer(i -> {
			closeLatch.countDown();
			return null;
		}).given(producer).close();
		ProducerFactory pf = mock(ProducerFactory.class);
		given(pf.transactionCapable()).willReturn(true);
		given(pf.createProducer()).willReturn(producer);
		KafkaTransactionManager tm = new KafkaTransactionManager(pf);
		ContainerProperties props = new ContainerProperties(new TopicPartitionInitialOffset("foo", 0),
				new TopicPartitionInitialOffset("foo", 1));
		props.setGroupId("group");
		props.setTransactionManager(tm);
		final KafkaTemplate template = new KafkaTemplate(pf);
		props.setMessageListener((BatchMessageListener) recordlist -> {
			template.send("bar", "baz");
			throw new RuntimeException("fail");
		});
		KafkaMessageListenerContainer container = new KafkaMessageListenerContainer<>(cf, props);
		container.setBeanName("rollback");
		container.start();
		assertThat(closeLatch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(seekLatch.await(10, TimeUnit.SECONDS)).isTrue();
		InOrder inOrder = inOrder(producer);
		inOrder.verify(producer).beginTransaction();
		ArgumentCaptor<ProducerRecord> captor = ArgumentCaptor.forClass(ProducerRecord.class);
		verify(producer).send(captor.capture(), any(Callback.class));
		assertThat(captor.getValue()).isEqualTo(new ProducerRecord("bar", "baz"));
		inOrder.verify(producer, never()).sendOffsetsToTransaction(anyMap(), anyString());
		inOrder.verify(producer, never()).commitTransaction();
		inOrder.verify(producer).abortTransaction();
		inOrder.verify(producer).close();
		verify(consumer).seek(topicPartition0, 0);
		verify(consumer).seek(topicPartition1, 0);
		verify(consumer, never()).commitSync(anyMap());
		container.stop();
		verify(pf, times(1)).createProducer();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testConsumeAndProduceTransactionExternalTM() throws Exception {
		Consumer consumer = mock(Consumer.class);
		final TopicPartition topicPartition = new TopicPartition("foo", 0);
		willAnswer(i -> {
			((ConsumerRebalanceListener) i.getArgument(1))
					.onPartitionsAssigned(Collections.singletonList(topicPartition));
			return null;
		}).given(consumer).subscribe(any(Collection.class), any(ConsumerRebalanceListener.class));
		final ConsumerRecords records = new ConsumerRecords(Collections.singletonMap(topicPartition,
				Collections.singletonList(new ConsumerRecord<>("foo", 0, 0, "key", "value"))));
		final AtomicBoolean done = new AtomicBoolean();
		willAnswer(i -> {
			if (done.compareAndSet(false, true)) {
				return records;
			}
			else {
				Thread.sleep(500);
				return null;
			}
		}).given(consumer).poll(any(Duration.class));
		ConsumerFactory cf = mock(ConsumerFactory.class);
		willReturn(consumer).given(cf).createConsumer("group", "", null);
		Producer producer = mock(Producer.class);

		final CountDownLatch closeLatch = new CountDownLatch(1);

		willAnswer(i -> {
			closeLatch.countDown();
			return null;
		}).given(producer).close();

		final ProducerFactory pf = mock(ProducerFactory.class);
		given(pf.transactionCapable()).willReturn(true);
		given(pf.createProducer()).willReturn(producer);
		ContainerProperties props = new ContainerProperties("foo");
		props.setGroupId("group");
		props.setTransactionManager(new SomeOtherTransactionManager());
		final KafkaTemplate template = new KafkaTemplate(pf);
		props.setMessageListener((MessageListener<String, String>) m -> {
			template.send("bar", "baz");
			template.sendOffsetsToTransaction(Collections.singletonMap(new TopicPartition(m.topic(), m.partition()),
					new OffsetAndMetadata(m.offset() + 1)));
		});
		KafkaMessageListenerContainer container = new KafkaMessageListenerContainer<>(cf, props);
		container.setBeanName("commit");
		container.start();

		assertThat(closeLatch.await(10, TimeUnit.SECONDS)).isTrue();

		InOrder inOrder = inOrder(producer);
		inOrder.verify(producer).beginTransaction();
		ArgumentCaptor<ProducerRecord> captor = ArgumentCaptor.forClass(ProducerRecord.class);
		inOrder.verify(producer).send(captor.capture(), any(Callback.class));
		assertThat(captor.getValue()).isEqualTo(new ProducerRecord("bar", "baz"));
		inOrder.verify(producer).sendOffsetsToTransaction(Collections.singletonMap(topicPartition,
				new OffsetAndMetadata(1)), "group");
		inOrder.verify(producer).commitTransaction();
		inOrder.verify(producer).close();
		container.stop();
		verify(pf).createProducer();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testRollbackRecord() throws Exception {
		logger.info("Start testRollbackRecord");
		Map<String, Object> props = KafkaTestUtils.consumerProps("txTest1", "false", embeddedKafka);
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "group");
		props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(props);
		ContainerProperties containerProps = new ContainerProperties(topic1, topic2);
		containerProps.setGroupId("group");
		containerProps.setPollTimeout(10_000);

		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		senderProps.put(ProducerConfig.RETRIES_CONFIG, 1);
		DefaultKafkaProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
		pf.setTransactionIdPrefix("rr.");

		final KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf);
		final AtomicBoolean failed = new AtomicBoolean();
		final CountDownLatch latch = new CountDownLatch(3);
		final AtomicReference<String> transactionalId = new AtomicReference<>();
		containerProps.setMessageListener((MessageListener<Integer, String>) message -> {
			latch.countDown();
			if (failed.compareAndSet(false, true)) {
				throw new RuntimeException("fail");
			}
			/*
			 * Send a message to topic2 and wait for it so we don't stop the container too soon.
			 */
			if (message.topic().equals(topic1)) {
				template.send(topic2, "bar");
				template.flush();
				transactionalId.set(KafkaTestUtils.getPropertyValue(
						ProducerFactoryUtils.getTransactionalResourceHolder(pf).getProducer(),
						"delegate.transactionManager.transactionalId", String.class));
			}
		});

		@SuppressWarnings({ "rawtypes" })
		KafkaTransactionManager tm = new KafkaTransactionManager(pf);
		containerProps.setTransactionManager(tm);
		KafkaMessageListenerContainer<Integer, String> container =
				new KafkaMessageListenerContainer<>(cf, containerProps);
		container.setBeanName("testRollbackRecord");
		container.start();

		template.setDefaultTopic(topic1);
		template.executeInTransaction(t -> {
			template.sendDefault(0, 0, "foo");
			return null;
		});
		assertThat(latch.await(60, TimeUnit.SECONDS)).isTrue();
		container.stop();
		Consumer<Integer, String> consumer = cf.createConsumer();
		final CountDownLatch subsLatch = new CountDownLatch(1);
		consumer.subscribe(Arrays.asList(topic1), new ConsumerRebalanceListener() {

			@Override
			public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
				// empty
			}

			@Override
			public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
				subsLatch.countDown();
			}

		});
		ConsumerRecords<Integer, String> records = null;
		int n = 0;
		while (subsLatch.getCount() > 0 && n++ < 600) {
			records = consumer.poll(Duration.ofMillis(100));
		}
		assertThat(subsLatch.await(1, TimeUnit.MILLISECONDS)).isTrue();
		assertThat(records.count()).isEqualTo(0);
		// depending on timing, the position might include the offset representing the commit in the log
		assertThat(consumer.position(new TopicPartition(topic1, 0))).isGreaterThanOrEqualTo(1L);
		assertThat(transactionalId.get()).startsWith("rr.group.txTopic");
		assertThat(KafkaTestUtils.getPropertyValue(pf, "consumerProducers", Map.class)).isEmpty();
		logger.info("Stop testRollbackRecord");
		pf.destroy();
		consumer.close();
	}

	@Test
	public void testMaxFailures() throws Exception {
		logger.info("Start testMaxFailures");
		Map<String, Object> props = KafkaTestUtils.consumerProps("txTestMaxFailures", "false", embeddedKafka);
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "group");
		props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(props);
		ContainerProperties containerProps = new ContainerProperties(topic3);
		containerProps.setPollTimeout(10_000);

		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		senderProps.put(ProducerConfig.RETRIES_CONFIG, 1);
		DefaultKafkaProducerFactory<Object, Object> pf = new DefaultKafkaProducerFactory<>(senderProps);
		pf.setTransactionIdPrefix("maxAtt.");
		final KafkaTemplate<Object, Object> template = new KafkaTemplate<>(pf);
		final CountDownLatch latch = new CountDownLatch(1);
		AtomicReference<String> data = new AtomicReference<>();
		containerProps.setMessageListener((MessageListener<Integer, String>) message -> {
			data.set(message.value());
			if (message.offset() == 0) {
				throw new RuntimeException("fail for max failures");
			}
			latch.countDown();
		});

		@SuppressWarnings({ "rawtypes", "unchecked" })
		KafkaTransactionManager tm = new KafkaTransactionManager(pf);
		containerProps.setTransactionManager(tm);
		KafkaMessageListenerContainer<Integer, String> container =
				new KafkaMessageListenerContainer<>(cf, containerProps);
		container.setBeanName("testMaxFailures");
		final CountDownLatch recoverLatch = new CountDownLatch(1);
		DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(template) {

			@Override
			public void accept(ConsumerRecord<?, ?> record, Exception exception) {
				super.accept(record, exception);
				recoverLatch.countDown();
			}

		};
		DefaultAfterRollbackProcessor<Integer, String> afterRollbackProcessor =
				spy(new DefaultAfterRollbackProcessor<>(recoverer, 3));
		container.setAfterRollbackProcessor(afterRollbackProcessor);
		final CountDownLatch stopLatch = new CountDownLatch(1);
		container.setApplicationEventPublisher(e -> {
			if (e instanceof ConsumerStoppedEvent) {
				stopLatch.countDown();
			}
		});
		container.start();

		template.setDefaultTopic(topic3);
		template.executeInTransaction(t -> {
			RecordHeaders headers = new RecordHeaders(new RecordHeader[] { new RecordHeader("baz", "qux".getBytes()) });
			ProducerRecord<Object, Object> record = new ProducerRecord<>(topic3, 0, 0, "foo", headers);
			template.send(record);
			template.sendDefault(0, 0, "bar");
			return null;
		});
		assertThat(latch.await(60, TimeUnit.SECONDS)).isTrue();
		assertThat(data.get()).isEqualTo("bar");
		assertThat(recoverLatch.await(10, TimeUnit.SECONDS)).isTrue();
		container.stop();
		Consumer<Integer, String> consumer = cf.createConsumer();
		embeddedKafka.consumeFromAnEmbeddedTopic(consumer, topic3DLT);
		ConsumerRecord<Integer, String> dltRecord = KafkaTestUtils.getSingleRecord(consumer, topic3DLT);
		assertThat(dltRecord.value()).isEqualTo("foo");
		DefaultKafkaHeaderMapper mapper = new DefaultKafkaHeaderMapper();
		Map<String, Object> map = new HashMap<>();
		mapper.toHeaders(dltRecord.headers(), map);
		MessageHeaders headers = new MessageHeaders(map);
		assertThat(new String(headers.get(KafkaHeaders.DLT_EXCEPTION_FQCN, byte[].class))).contains("RuntimeException");
		assertThat(headers.get(KafkaHeaders.DLT_EXCEPTION_MESSAGE, byte[].class))
				.isEqualTo("fail for max failures".getBytes());
		assertThat(headers.get(KafkaHeaders.DLT_EXCEPTION_STACKTRACE)).isNotNull();
		assertThat(headers.get(KafkaHeaders.DLT_ORIGINAL_OFFSET, byte[].class)[3]).isEqualTo((byte) 0);
		assertThat(headers.get(KafkaHeaders.DLT_ORIGINAL_PARTITION, byte[].class)[3]).isEqualTo((byte) 0);
		assertThat(headers.get(KafkaHeaders.DLT_ORIGINAL_TIMESTAMP, byte[].class)).isNotNull();
		assertThat(headers.get(KafkaHeaders.DLT_ORIGINAL_TIMESTAMP_TYPE, byte[].class)).isNotNull();
		assertThat(headers.get(KafkaHeaders.DLT_ORIGINAL_TOPIC, byte[].class)).isEqualTo(topic3.getBytes());
		assertThat(headers.get("baz")).isEqualTo("qux".getBytes());
		pf.destroy();
		assertThat(stopLatch.await(10, TimeUnit.SECONDS)).isTrue();
		verify(afterRollbackProcessor).clearThreadState();
		logger.info("Stop testMaxAttempts");
	}

	@SuppressWarnings("serial")
	public static class SomeOtherTransactionManager extends AbstractPlatformTransactionManager {

		@Override
		protected Object doGetTransaction() throws TransactionException {
			return new Object();
		}

		@Override
		protected void doBegin(Object transaction, TransactionDefinition definition) throws TransactionException {
			//noop
		}

		@Override
		protected void doCommit(DefaultTransactionStatus status) throws TransactionException {
			//noop
		}

		@Override
		protected void doRollback(DefaultTransactionStatus status) throws TransactionException {
			//noop
		}

	}

}
