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

package org.springframework.kafka.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.kafka.test.assertj.KafkaConditions.key;
import static org.springframework.kafka.test.assertj.KafkaConditions.partition;
import static org.springframework.kafka.test.assertj.KafkaConditions.value;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.ClassRule;
import org.junit.Test;

import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.ProducerListenerAdapter;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.messaging.support.MessageBuilder;


/**
 * @author Gary Russell
 * @author Artem Bilan
 *
 */
public class KafkaTemplateTests {

	private static final String TEMPLATE_TOPIC = "templateTopic";

	@ClassRule
	public static KafkaEmbedded embeddedKafka = new KafkaEmbedded(1, true, TEMPLATE_TOPIC);

	@Test
	public void testTemplate() throws Exception {
		Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("testT", "false", embeddedKafka);
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<Integer, String>(
				consumerProps);
		Consumer<Integer, String> consumer = cf.createConsumer();
		embeddedKafka.consumeFromAllEmbeddedTopics(consumer);

		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		ProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<Integer, String>(senderProps);
		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf);
		template.setDefaultTopic(TEMPLATE_TOPIC);
		template.syncSend("foo");
		assertThat(KafkaTestUtils.getSingleRecord(consumer, TEMPLATE_TOPIC)).has(value("foo"));
		template.syncSend(0, 2, "bar");
		ConsumerRecord<Integer, String> received = KafkaTestUtils.getSingleRecord(consumer, TEMPLATE_TOPIC);
		assertThat(received).has(key(2));
		assertThat(received).has(partition(0));
		assertThat(received).has(value("bar"));
		template.syncSend(TEMPLATE_TOPIC, 0, 2, "baz");
		received = KafkaTestUtils.getSingleRecord(consumer, TEMPLATE_TOPIC);
		assertThat(received).has(key(2));
		assertThat(received).has(partition(0));
		assertThat(received).has(value("baz"));
		template.syncSend(TEMPLATE_TOPIC, 0, "qux");
		received = KafkaTestUtils.getSingleRecord(consumer, TEMPLATE_TOPIC);
		assertThat(received).has(key((Integer) null));
		assertThat(received).has(partition(0));
		assertThat(received).has(value("qux"));
		template.syncConvertAndSend(MessageBuilder.withPayload("fiz")
				.setHeader(KafkaHeaders.TOPIC, TEMPLATE_TOPIC)
				.setHeader(KafkaHeaders.PARTITION_ID, 0)
				.setHeader(KafkaHeaders.MESSAGE_KEY, 2)
				.build());
		received = KafkaTestUtils.getSingleRecord(consumer, TEMPLATE_TOPIC);
		assertThat(received).has(key(2));
		assertThat(received).has(partition(0));
		assertThat(received).has(value("fiz"));
		template.syncConvertAndSend(MessageBuilder.withPayload("buz")
				.setHeader(KafkaHeaders.PARTITION_ID, 0)
				.setHeader(KafkaHeaders.MESSAGE_KEY, 2)
				.build());
		received = KafkaTestUtils.getSingleRecord(consumer, TEMPLATE_TOPIC);
		assertThat(received).has(key(2));
		assertThat(received).has(partition(0));
		assertThat(received).has(value("buz"));

		consumer.close();
	}

	@Test
	public void withListener() throws Exception {
		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		ProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<Integer, String>(senderProps);
		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf);
		template.setDefaultTopic(TEMPLATE_TOPIC);
		final CountDownLatch latch = new CountDownLatch(1);
		template.setProducerListener(new ProducerListenerAdapter<Integer, String>() {

			@Override
			public void onSuccess(String topic, Integer partition, Integer key, String value,
					RecordMetadata recordMetadata) {
				latch.countDown();
			}

		});
		template.syncSend("foo");
		template.flush();
		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
	}

}
