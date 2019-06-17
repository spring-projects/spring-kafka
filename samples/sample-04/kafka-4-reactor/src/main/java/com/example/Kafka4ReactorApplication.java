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

import java.lang.reflect.Method;
import java.util.stream.IntStream;

import org.apache.kafka.clients.admin.NewTopic;

import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate;
import org.springframework.kafka.listener.reactive.ReactorAdapter;
import org.springframework.stereotype.Component;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderResult;

@SpringBootApplication
public class Kafka4ReactorApplication {

	public static void main(String[] args) {
		SpringApplication.run(Kafka4ReactorApplication.class, args);
	}

	@Bean
	public ReactorAdapter adapter(Listener listener, KafkaProperties properties)
			throws NoSuchMethodException, SecurityException {

		// This will be done by the bean post processor
		Method method = Listener.class.getDeclaredMethod("listen", Flux.class);
		ReactorAdapter adapter = new ReactorAdapter(listener, method, "skReactorTopic");
		adapter.setConfigs(properties.buildConsumerProperties());
		return adapter;
	}

	@Bean
	public ReactiveKafkaProducerTemplate<String, String> template(KafkaProperties properties) {
		SenderOptions<String, String> senderOptions = SenderOptions.create(properties.buildProducerProperties());
		return new ReactiveKafkaProducerTemplate<>(senderOptions);
	}

	@Bean
	public NewTopic topic() {
		return TopicBuilder.name("skReactorTopic").partitions(1).replicas(1).build();
	}

	@Bean
	public ApplicationRunner runner(ReactiveKafkaProducerTemplate<String, String> template) {
		return args -> IntStream.range(0, 10).forEach(i -> {
			Mono<SenderResult<Void>> send = template.send("skReactorTopic", "foo", "bar" + i);
			send.subscribe(sr -> System.out.println(sr.recordMetadata()));
		});
	}

}

@Component
class Listener {

	@KafkaListener(topics = "skReactorTopic")
	public Mono<Void> listen(Flux<ReceiverRecord<String, String>> flux) {
		return flux.doOnNext(record -> {
			System.out.println(record.key() + ":" + record.value() + "@" + record.offset());
			record.receiverOffset().acknowledge();
		}).then();
	}

}

