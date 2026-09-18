/*
 * Copyright 2026-present the original author or authors.
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

package com.example.sample09;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ShareKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultShareConsumerFactory;
import org.springframework.kafka.core.ShareConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;

/**
 * Share consumer infrastructure for this sample. Spring Boot does not auto-configure share
 * consumers, so the {@link ShareConsumerFactory} and the
 * {@link ShareKafkaListenerContainerFactory} that {@code @KafkaListener} refers to are declared here.
 *
 * @author Omar Morales Ortega
 *
 * @since 4.2
 */

@Configuration(proxyBeanMethods = false)
public class ShareConsumerConfiguration {

	@Bean
	ShareConsumerFactory<String, String> shareConsumerFactory(KafkaProperties kafkaProperties) {
		Map<String, Object> props = new HashMap<>();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		return new DefaultShareConsumerFactory<>(props);
	}

	@Bean
	ShareKafkaListenerContainerFactory<String, String> manualShareKafkaListenerContainerFactory(
			ShareConsumerFactory<String, String> shareConsumerFactory) {

		ShareKafkaListenerContainerFactory<String, String> factory =
				new ShareKafkaListenerContainerFactory<>(shareConsumerFactory);
		factory.getContainerProperties().setShareAckMode(ContainerProperties.ShareAckMode.MANUAL);
		return factory;
	}

	@Bean
	ShareGroupInitializer shareGroupInitializer(KafkaProperties kafkaProperties,
			@Value("${sample09.group}") String groupId) {

		return new ShareGroupInitializer(kafkaProperties.getBootstrapServers(), groupId);
	}

}
