/*
 * Copyright 2018-2022 the original author or authors.
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

import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.converter.JsonMessageConverter;
import org.springframework.kafka.support.converter.RecordMessageConverter;
import org.springframework.util.backoff.FixedBackOff;

import com.common.Foo2;

/**
 * Sample shows use of a dead letter topic.
 *
 * @author Gary Russell
 * @since 2.2.1
 *
 */
@SpringBootApplication
public class Application {

	private final Logger logger = LoggerFactory.getLogger(Application.class);

	private final TaskExecutor exec = new SimpleAsyncTaskExecutor();

	public static void main(String[] args) {
		SpringApplication.run(Application.class, args).close();
	}

	/*
	 * Boot will autowire this into the container factory.
	 */
	@Bean
	public CommonErrorHandler errorHandler(KafkaOperations<Object, Object> template) {
		return new DefaultErrorHandler(
				new DeadLetterPublishingRecoverer(template), new FixedBackOff(1000L, 2));
	}

	@Bean
	public RecordMessageConverter converter() {
		return new JsonMessageConverter();
	}

	@KafkaListener(id = "fooGroup", topics = "topic1")
	public void listen(Foo2 foo) {
		logger.info("Received: " + foo);
		if (foo.getFoo().startsWith("fail")) {
			throw new RuntimeException("failed");
		}
		this.exec.execute(() -> System.out.println("Hit Enter to terminate..."));
	}

	@KafkaListener(id = "dltGroup", topics = "topic1.DLT")
	public void dltListen(byte[] in) {
		logger.info("Received from DLT: " + new String(in));
		this.exec.execute(() -> System.out.println("Hit Enter to terminate..."));
	}

	@Bean
	public NewTopic topic() {
		return new NewTopic("topic1", 1, (short) 1);
	}

	@Bean
	public NewTopic dlt() {
		return new NewTopic("topic1.DLT", 1, (short) 1);
	}

	@Bean
	@Profile("default") // Don't run from test(s)
	public ApplicationRunner runner() {
		return args -> {
			System.out.println("Hit Enter to terminate...");
			System.in.read();
		};
	}

}
