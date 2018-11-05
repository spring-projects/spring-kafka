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

import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.SeekToCurrentErrorHandler;
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
		factory.setErrorHandler(new SeekToCurrentErrorHandler(
				new DeadLetterPublishingRecoverer(template), 3)); // dead-letter after 3 tries
		return factory;
	}

	@Bean
	public RecordMessageConverter converter() {
		return new StringJsonMessageConverter();
	}

	@KafkaListener(id = "fooGroup", topics = "topic1")
	public void listen(Foo2 foo) {
		logger.info("Received: " + foo);
		if (foo.getFoo().startsWith("fail")) {
			throw new RuntimeException("failed");
		}
	}

	@KafkaListener(id = "dltGroup", topics = "topic1.DLT")
	public void dltListen(String in) {
		logger.info("Received from DLT: " + in);
	}

	@Bean
	public NewTopic topic() {
		return new NewTopic("topic1", 1, (short) 1);
	}

	@Bean
	public NewTopic dlt() {
		return new NewTopic("topic1.DLT", 1, (short) 1);
	}

}
