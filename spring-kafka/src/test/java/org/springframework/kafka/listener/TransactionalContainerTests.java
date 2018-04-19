/*
 * Copyright 2017 the original author or authors.
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
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.BDDMockito.willReturn;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

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
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;

import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.AbstractMessageListenerContainer.AckMode;
import org.springframework.kafka.listener.config.ContainerProperties;
import org.springframework.kafka.support.TopicPartitionInitialOffset;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.kafka.transaction.KafkaTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;

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

	@ClassRule
	public static KafkaEmbedded embeddedKafka = new KafkaEmbedded(3, true, topic1, topic2);

	@Test
	public void testConsumeAndProduceTransactionBatch() throws Exception {
		testConsumeAndProduceTransactionGuts(AckMode.BATCH);
	}

	@Test
	public void testConsumeAndProduceTransactionRecord() throws Exception {
		testConsumeAndProduceTransactionGuts(AckMode.RECORD);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private void testConsumeAndProduceTransactionGuts(AckMode ackMode) throws Exception {
		Consumer consumer = mock(Consumer.class);
		final TopicPartition topicPartition = new TopicPartition("foo", 0);
		willAnswer(i -> {
			i.getArgumentAt(1, ConsumerRebalanceListener.class)
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
		}).given(consumer).poll(anyLong());
		ConsumerFactory cf = mock(ConsumerFactory.class);
		willReturn(consumer).given(cf).createConsumer("group", null);
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
		ContainerProperties props = new ContainerProperties("foo");
		props.setGroupId("group");
		props.setTransactionManager(tm);
		final KafkaTemplate template = new KafkaTemplate(pf);
		props.setMessageListener((MessageListener) m -> {
			template.send("bar", "baz");
		});
		props.setAckMode(ackMode);
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
		verify(pf, times(1)).createProducer();
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
		}).given(consumer).poll(anyLong());
		final CountDownLatch seekLatch = new CountDownLatch(2);
		willAnswer(i -> {
			seekLatch.countDown();
			return null;
		}).given(consumer).seek(any(), anyLong());
		ConsumerFactory cf = mock(ConsumerFactory.class);
		willReturn(consumer).given(cf).createConsumer("group", null);
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
		verify(consumer, never()).commitSync(any());
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
		}).given(consumer).poll(anyLong());
		final CountDownLatch seekLatch = new CountDownLatch(2);
		willAnswer(i -> {
			seekLatch.countDown();
			return null;
		}).given(consumer).seek(any(), anyLong());
		ConsumerFactory cf = mock(ConsumerFactory.class);
		willReturn(consumer).given(cf).createConsumer("group", null);
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
		verify(consumer, never()).commitSync(any());
		container.stop();
		verify(pf, times(1)).createProducer();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testConsumeAndProduceTransactionExternalTM() throws Exception {
		Consumer consumer = mock(Consumer.class);
		final TopicPartition topicPartition = new TopicPartition("foo", 0);
		willAnswer(i -> {
			i.getArgumentAt(1, ConsumerRebalanceListener.class)
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
		}).given(consumer).poll(anyLong());
		ConsumerFactory cf = mock(ConsumerFactory.class);
		willReturn(consumer).given(cf).createConsumer("group", null);
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
			}
		});

		@SuppressWarnings({ "rawtypes", "unchecked" })
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
		ConsumerRecords<Integer, String> records = consumer.poll(0);
		assertThat(records.count()).isEqualTo(0);
		assertThat(consumer.position(new TopicPartition(topic1, 0))).isEqualTo(1);
		logger.info("Stop testRollbackRecord");
		pf.destroy();
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
