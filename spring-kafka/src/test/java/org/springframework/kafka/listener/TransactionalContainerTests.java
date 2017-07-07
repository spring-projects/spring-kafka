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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.BDDMockito.willReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.config.ContainerProperties;
import org.springframework.kafka.transaction.KafkaTransactionManager;

/**
 * @author Gary Russell
 * @since 2.0
 *
 */
public class TransactionalContainerTests {

	private final Log logger = LogFactory.getLog(this.getClass());

//	private static String topic1 = "txTopic1";
//
//	private static String topic2 = "txTopic2";
//
//	@ClassRule
//	public static KafkaEmbedded embeddedKafka = new KafkaEmbedded(3, true, topic1, topic2);

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testConsumeAndProduceTransaction() throws Exception {
		Consumer consumer = mock(Consumer.class);
		final TopicPartition topicPartition = new TopicPartition("foo", 0);
		willAnswer(i -> {
			((ConsumerRebalanceListener) i.getArgument(1))
					.onPartitionsAssigned(Collections.singletonList(topicPartition));
			return null;
		}).given(consumer).subscribe(any(Collection.class), any(ConsumerRebalanceListener.class));
		ConsumerRecords records = new ConsumerRecords(Collections.singletonMap(topicPartition,
				Collections.singletonList(new ConsumerRecord<>("foo", 0, 0, "key", "value"))));
		given(consumer.poll(anyLong())).willReturn(records, (ConsumerRecords) null);
		ConsumerFactory cf = mock(ConsumerFactory.class);
		willReturn(consumer).given(cf).createConsumer("group", null);
		Producer producer = mock(Producer.class);
		final CountDownLatch commitLatch = new CountDownLatch(1);
		willAnswer(i -> {
			commitLatch.countDown();
			return null;
		}).given(producer).commitTransaction();
		ProducerFactory pf = mock(ProducerFactory.class);
		given(pf.createProducer()).willReturn(producer);
		KafkaTransactionManager tm = new KafkaTransactionManager(pf);
		ContainerProperties props = new ContainerProperties("foo");
		props.setGroupId("group");
		props.setTransactionManager(tm);
		final KafkaTemplate template = new KafkaTemplate(pf);
		props.setMessageListener((MessageListener) m -> {
			template.send("bar", "baz");
		});
		KafkaMessageListenerContainer container = new KafkaMessageListenerContainer<>(cf, props);
		container.start();
		assertThat(commitLatch.await(10, TimeUnit.SECONDS)).isTrue();
		verify(producer).beginTransaction();
		ArgumentCaptor<ProducerRecord> captor = ArgumentCaptor.forClass(ProducerRecord.class);
		verify(producer).send(captor.capture(), any(Callback.class));
		assertThat(captor.getValue()).isEqualTo(new ProducerRecord("bar", "baz"));
		verify(producer).sendOffsetsToTransaction(Collections.singletonMap(topicPartition,
				new OffsetAndMetadata(1)), "group");
		verify(producer).commitTransaction();
	}

}
