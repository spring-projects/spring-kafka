/*
 * Copyright 2018 the original author or authors.
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

package com.example;

import java.io.IOException;
import java.util.List;

import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.converter.BatchMessagingMessageConverter;
import org.springframework.kafka.support.converter.RecordMessageConverter;
import org.springframework.kafka.support.converter.StringJsonMessageConverter;

import com.common.Foo2;

/**
 *
 * @author Gary Russell
 * @since 2.2.1
 *
 */
@SpringBootApplication
public class Application {

	private final Logger logger = LoggerFactory.getLogger(Application.class);

	public static void main(String[] args) {
		SpringApplication.run(Application.class, args);
	}

	@Bean
	public ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(
			ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
			ConsumerFactory<Object, Object> kafkaConsumerFactory,
			KafkaTemplate<Object, Object> template) {
		ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
		configurer.configure(factory, kafkaConsumerFactory);
		factory.setBatchListener(true);
		factory.setMessageConverter(batchConverter());
		return factory;
	}

	@Bean
	public RecordMessageConverter converter() {
		return new StringJsonMessageConverter();
	}

	@Bean
	public BatchMessagingMessageConverter batchConverter() {
		return new BatchMessagingMessageConverter(converter());
	}

	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;

	@KafkaListener(id = "fooGroup2", topics = "topic2")
	public void listen1(List<Foo2> foos) throws IOException {
		logger.info("Received: " + foos);
		foos.forEach(f -> kafkaTemplate.send("topic3", f.getFoo().toUpperCase()));
		logger.info("Messages sent, hit Enter to commit tx");
		System.in.read();
	}

	@KafkaListener(id = "fooGroup3", topics = "topic3")
	public void listen2(List<String> in) {
		logger.info("Received: " + in);
	}

	@Bean
	public NewTopic topic() {
		return new NewTopic("topic2", 1, (short) 1);
	}

}
