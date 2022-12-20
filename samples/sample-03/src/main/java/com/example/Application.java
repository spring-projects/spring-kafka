/*
 * Copyright 2018-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
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
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.converter.BatchMessagingMessageConverter;
import org.springframework.kafka.support.converter.JsonMessageConverter;
import org.springframework.kafka.support.converter.RecordMessageConverter;
import org.springframework.stereotype.Component;

import com.common.Foo2;

/**
 * Sample showing a batch listener and transactions.
 *
 * @author Gary Russell
 * @since 2.2.1
 *
 */
@SpringBootApplication
public class Application {

	final static CountDownLatch LATCH = new CountDownLatch(1);

	public static void main(String[] args) throws InterruptedException {
		ConfigurableApplicationContext context = SpringApplication.run(Application.class, args);
		LATCH.await();
		Thread.sleep(5_000);
		context.close();
	}

	@Bean
	public RecordMessageConverter converter() {
		return new JsonMessageConverter();
	}

	@Bean
	public BatchMessagingMessageConverter batchConverter() {
		return new BatchMessagingMessageConverter(converter());
	}

	@Bean
	public NewTopic topic2() {
		return TopicBuilder.name("topic2").partitions(1).replicas(1).build();
	}

	@Bean
	public NewTopic topic3() {
		return TopicBuilder.name("topic3").partitions(1).replicas(1).build();
	}

}

@Component
class Listener {

	private static final Logger LOGGER = LoggerFactory.getLogger(Listener.class);

	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;

	@KafkaListener(id = "fooGroup2", topics = "topic2")
	public void listen1(List<Foo2> foos) throws IOException {
		LOGGER.info("Received: " + foos);
		foos.forEach(f -> kafkaTemplate.send("topic3", f.getFoo().toUpperCase()));
		LOGGER.info("Messages sent, hit Enter to commit tx");
		System.in.read();
	}

	@KafkaListener(id = "fooGroup3", topics = "topic3")
	public void listen2(List<String> in) {
		LOGGER.info("Received: " + in);
		Application.LATCH.countDown();
	}

}
