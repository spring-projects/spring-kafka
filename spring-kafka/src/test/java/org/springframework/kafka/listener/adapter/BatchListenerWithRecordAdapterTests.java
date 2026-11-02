/*
 * Copyright 2019-present the original author or authors.
 */

package org.springframework.kafka.listener.adapter;

import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * @author Gary Russell
 * @author Soby Chacko
 * @since 2.2.5
 *
 */
@SpringJUnitConfig
@DirtiesContext
public class BatchListenerWithRecordAdapterTests {

	@SuppressWarnings("unchecked")
	@Test
	void test(@Autowired KafkaListenerEndpointRegistry registry, @Autowired TestListener foo, @Autowired Config config) {
		BatchMessagingMessageListenerAdapter<String, String> adapter =
				(BatchMessagingMessageListenerAdapter<String, String>) registry
					.getListenerContainer("batchRecordAdapter").getContainerProperties().getMessageListener();
		List<ConsumerRecord<String, String>> records = new ArrayList<>();
		records.add(new ConsumerRecord<>("foo", 0, 0, null, "foo"));
		ConsumerRecord<String, String> barRecord = new ConsumerRecord<>("foo", 0, 1, null, "bar");
		records.add(barRecord);
		records.add(new ConsumerRecord<String, String>("foo", 0, 2, null, "baz"));
		adapter.onMessage(records, null, null);
		assertThat(foo.values1).contains("foo", "bar", "baz");
		assertThat(config.failed).isSameAs(barRecord);
	}

	@SuppressWarnings("unchecked")
	@Test
	void testFullRecord(@Autowired KafkaListenerEndpointRegistry registry, @Autowired TestListener foo,
			@Autowired Config config) {

		config.failed = null;
		BatchMessagingMessageListenerAdapter<String, String> adapter =
				(BatchMessagingMessageListenerAdapter<String, String>) registry
					.getListenerContainer("batchRecordAdapterFullRecord").getContainerProperties().getMessageListener();
		List<ConsumerRecord<String, String>> records = new ArrayList<>();
		records.add(new ConsumerRecord<>("foo", 0, 0, null, "foo"));
		ConsumerRecord<String, String> barRecord = new ConsumerRecord<String, String>("foo", 0, 1, null, "bar");
		records.add(barRecord);
		records.add(new ConsumerRecord<>("foo", 0, 2, null, "baz"));
		adapter.onMessage(records, null, null);
		assertThat(foo.values2).contains("foo", "bar", "baz");
		assertThat(config.failed).isNull();
	}

	public static class TestListener {

		final List<String> values1 = new ArrayList<>();

		final List<String> values2 = new ArrayList<>();

		@KafkaListener(id = "batchRecordAdapter", topics = "foo", autoStartup = "false")
		public void listen1(String data) {
			values1.add(data);
			if ("bar".equals(data)) {
				throw new RuntimeException("reject partial");
			}
		}

		@KafkaListener(id = "batchRecordAdapterFullRecord", topics = "foo", autoStartup = "false")
		public void listen2(ConsumerRecord<Integer, String> data) {
			values2.add(data.value());
		}

	}

	@Configuration
	@EnableKafka
	public static class Config {

		ConsumerRecord<?, ?> failed;

		@Bean
		public TestListener foo() {
			return new TestListener();
		}

		@Bean
		public ConsumerFactory<?, ?> consumerFactory() {
			return mock(ConsumerFactory.class);
		}

		@SuppressWarnings({ "rawtypes", "unchecked" })
		@Bean
		public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
			ConcurrentKafkaListenerContainerFactory factory = new ConcurrentKafkaListenerContainerFactory();
			factory.setConsumerFactory(consumerFactory());
			factory.setBatchListener(true);
			factory.setBatchToRecordAdapter(new DefaultBatchToRecordAdapter<>((record, ex) ->  {
				this.failed = record;
			}));
			return factory;
		}
	}

}
