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

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.ClassRule;
import org.junit.Test;

import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.messaging.MessageDeliveryException;


/**
 * @author Martin Dam
 *
 */
public class KafkaMessageListenerContainerTests {
	private final Log logger = LogFactory.getLog(this.getClass());

	private static String topic1 = "testTopic1";

	@ClassRule
	public static KafkaEmbedded embeddedKafka = new KafkaEmbedded(1, true, topic1);

	@Test
	public void testListenerDeliveryException() throws Exception {
		logger.info("Start exception");

		final CountDownLatch pauseLatch = new CountDownLatch(2);
		final CountDownLatch resumeLatch = new CountDownLatch(2);
		final CountDownLatch emitLatch = new CountDownLatch(4);

		Map<String, Object> props = KafkaTestUtils.consumerProps("test1", "true", embeddedKafka);
		final Consumer<Integer, String> kafkaConsumer = new KafkaConsumer<Integer, String>(props) {

			@Override
			public void pause(TopicPartition... partitions) {
				logger.info("Pausing");
				pauseLatch.countDown();
				super.pause(partitions);
			}

			@Override
			public void resume(TopicPartition... partitions) {
				logger.info("Resuming");
				resumeLatch.countDown();
				super.resume(partitions);
			}

			@Override
			public ConsumerRecords<Integer, String> poll(long timeout) {
				logger.info("Polling");
				return super.poll(timeout);
			}
		};
		ConsumerFactory<Integer, String> cf = new ConsumerFactory<Integer, String>() {

			@Override
			public Consumer<Integer, String> createConsumer() {
				return kafkaConsumer;
			}

			@Override
			public boolean isAutoCommit() {
				return false;
			}
		};


		KafkaMessageListenerContainer<Integer, String> container =
				new KafkaMessageListenerContainer<>(cf, topic1);

		container.setMessageListener(new MessageListener<Integer, String>() {
			@Override
			public void onMessage(ConsumerRecord<Integer, String> message) {
				logger.info("onMessage: " + message);
				emitLatch.countDown();
				if (emitLatch.getCount() % 2 == 1) {
					throw new MessageDeliveryException("intended");
				}
			}
		});
		container.setBeanName("testException");
		container.setPollTimeout(10);
		container.start();

		ContainerTestUtils.waitForAssignment(container, embeddedKafka.getPartitionsPerTopic());
		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		ProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<Integer, String>(senderProps);
		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf);
		template.setDefaultTopic(topic1);
		template.sendDefault(0, "foo");
		template.sendDefault(2, "bar");
		template.flush();
		assertThat(emitLatch.await(60, TimeUnit.SECONDS)).isTrue();
		assertThat(resumeLatch.await(60, TimeUnit.SECONDS)).isTrue();
		assertThat(pauseLatch.await(60, TimeUnit.SECONDS)).isTrue();
		container.stop();
		logger.info("Stop exception");

	}

}
