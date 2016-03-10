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
import static org.springframework.kafka.test.assertj.KafkaConditions.hasKey;
import static org.springframework.kafka.test.assertj.KafkaConditions.hasPartition;
import static org.springframework.kafka.test.assertj.KafkaConditions.hasValue;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.ClassRule;
import org.junit.Test;

import org.springframework.kafka.listener.ContainerTestUtils;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.kafka.test.utils.KafkaTestUtils;


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
		KafkaMessageListenerContainer<Integer, String> container = new KafkaMessageListenerContainer<>(cf,
				TEMPLATE_TOPIC);
		final BlockingQueue<ConsumerRecord<Integer, String>> records = new LinkedBlockingQueue<>();
		container.setMessageListener(new MessageListener<Integer, String>() {

			@Override
			public void onMessage(ConsumerRecord<Integer, String> record) {
				System.out.println(record);
				records.add(record);
			}

		});
		container.setBeanName("templateTests");
		container.start();
		ContainerTestUtils.waitForAssignment(container, embeddedKafka.getPartitionsPerTopic());
		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		ProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<Integer, String>(senderProps);
		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf);
		template.setDefaultTopic(TEMPLATE_TOPIC);
		template.syncConvertAndSend("foo");
		assertThat(records.poll(10, TimeUnit.SECONDS)).is(hasValue("foo"));
		template.syncConvertAndSend(0, 2, "bar");
		ConsumerRecord<Integer, String> received = records.poll(10, TimeUnit.SECONDS);
		assertThat(received).is(hasKey(2));
		assertThat(received).is(hasPartition(0));
		assertThat(received).is(hasValue("bar"));
		template.syncConvertAndSend(TEMPLATE_TOPIC, 0, 2, "baz");
		received = records.poll(10, TimeUnit.SECONDS);
		assertThat(received).is(hasKey(2));
		assertThat(received).is(hasPartition(0));
		assertThat(received).is(hasValue("baz"));
	}

}
