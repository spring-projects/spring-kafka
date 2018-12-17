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

package org.springframework.kafka.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.listener.config.ContainerProperties;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.kafka.transaction.KafkaTransactionManager;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.util.concurrent.ListenableFuture;

/**
 * @author Gary Russell
 * @since 1.0.6
 *
 */
@EmbeddedKafka(topics = { "txCache1", "txCache2", "txCacheSendFromListener" },
		brokerProperties = {
				"transaction.state.log.replication.factor=1",
				"transaction.state.log.min.isr=1" }
)
@RunWith(SpringRunner.class)
@DirtiesContext
public class DefaultKafkaConsumerFactoryTests {

	@Autowired
	private KafkaEmbedded embeddedKafka;

	@Test
	public void testClientId() {
		Map<String, Object> configs = Collections.singletonMap(ConsumerConfig.CLIENT_ID_CONFIG, "foo");
		DefaultKafkaConsumerFactory<String, String> factory =
				new DefaultKafkaConsumerFactory<String, String>(configs) {

			@Override
			protected KafkaConsumer<String, String> createKafkaConsumer(Map<String, Object> configs) {
				assertThat(configs.get(ConsumerConfig.CLIENT_ID_CONFIG)).isEqualTo("foo-1");
				return null;
			}

		};
		factory.createConsumer("-1");
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testNestedTxProducerIsCached() throws Exception {
		Map<String, Object> producerProps = KafkaTestUtils.producerProps(this.embeddedKafka);
		producerProps.put(ProducerConfig.RETRIES_CONFIG, 1);
		DefaultKafkaProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(producerProps);
		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf);
		DefaultKafkaProducerFactory<Integer, String> pfTx = new DefaultKafkaProducerFactory<>(producerProps);
		pfTx.setTransactionIdPrefix("fooTx.");
		KafkaTemplate<Integer, String> templateTx = new KafkaTemplate<>(pfTx);
		Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("txCache1Group", "false", this.embeddedKafka);
		consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
		ContainerProperties containerProps = new ContainerProperties("txCache1");
		CountDownLatch latch = new CountDownLatch(1);
		containerProps.setMessageListener((MessageListener<Integer, String>) r -> {
			templateTx.executeInTransaction(t -> t.send("txCacheSendFromListener", "bar"));
			templateTx.executeInTransaction(t -> t.send("txCacheSendFromListener", "baz"));
			latch.countDown();
		});
		KafkaTransactionManager<Integer, String> tm = new KafkaTransactionManager<>(pfTx);
		containerProps.setTransactionManager(tm);
		KafkaMessageListenerContainer<Integer, String> container = new KafkaMessageListenerContainer<>(cf,
				containerProps);
		container.start();
		try {
			ListenableFuture<SendResult<Integer, String>> future = template.send("txCache1", "foo");
			future.get();
			assertThat(KafkaTestUtils.getPropertyValue(pf, "cache", BlockingQueue.class)).hasSize(0);
			assertThat(latch.await(30, TimeUnit.SECONDS)).isTrue();
			assertThat(KafkaTestUtils.getPropertyValue(pfTx, "cache", BlockingQueue.class)).hasSize(1);
		}
		finally {
			container.stop();
			pf.destroy();
			pfTx.destroy();
		}
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testContainerTxProducerIsNotCached() throws Exception {
		Map<String, Object> producerProps = KafkaTestUtils.producerProps(this.embeddedKafka);
		producerProps.put(ProducerConfig.RETRIES_CONFIG, 1);
		DefaultKafkaProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(producerProps);
		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf);
		DefaultKafkaProducerFactory<Integer, String> pfTx = new DefaultKafkaProducerFactory<>(producerProps);
		pfTx.setTransactionIdPrefix("fooTx.");
		KafkaTemplate<Integer, String> templateTx = new KafkaTemplate<>(pfTx);
		Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("txCache2Group", "false", this.embeddedKafka);
		consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
		ContainerProperties containerProps = new ContainerProperties("txCache2");
		CountDownLatch latch = new CountDownLatch(1);
		containerProps.setMessageListener((MessageListener<Integer, String>) r -> {
			templateTx.send("txCacheSendFromListener", "bar");
			templateTx.send("txCacheSendFromListener", "baz");
			latch.countDown();
		});
		KafkaTransactionManager<Integer, String> tm = new KafkaTransactionManager<>(pfTx);
		containerProps.setTransactionManager(tm);
		KafkaMessageListenerContainer<Integer, String> container = new KafkaMessageListenerContainer<>(cf,
				containerProps);
		container.start();
		try {
			ListenableFuture<SendResult<Integer, String>> future = template.send("txCache2", "foo");
			future.get();
			assertThat(KafkaTestUtils.getPropertyValue(pf, "cache", BlockingQueue.class)).hasSize(0);
			assertThat(latch.await(30, TimeUnit.SECONDS)).isTrue();
			assertThat(KafkaTestUtils.getPropertyValue(pfTx, "cache", BlockingQueue.class)).hasSize(0);
		}
		finally {
			container.stop();
			pf.destroy();
			pfTx.destroy();
		}
	}

	@Configuration
	public static class Config {

	}

}
