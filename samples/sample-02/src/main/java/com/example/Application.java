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

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.admin.NewTopic;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.SeekToCurrentErrorHandler;
import org.springframework.kafka.support.converter.DefaultJackson2JavaTypeMapper;
import org.springframework.kafka.support.converter.Jackson2JavaTypeMapper.TypePrecedence;
import org.springframework.kafka.support.converter.RecordMessageConverter;
import org.springframework.kafka.support.converter.StringJsonMessageConverter;

import com.common.Bar2;
import com.common.Foo2;

@SpringBootApplication
public class Application {

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
				new DeadLetterPublishingRecoverer(template), 3));
		return factory;
	}

	@Bean
	public RecordMessageConverter converter() {
		StringJsonMessageConverter converter = new StringJsonMessageConverter();
		DefaultJackson2JavaTypeMapper typeMapper = new DefaultJackson2JavaTypeMapper();
		typeMapper.setTypePrecedence(TypePrecedence.TYPE_ID);
		typeMapper.addTrustedPackages("com.common");
		Map<String, Class<?>> mappings = new HashMap<>();
		mappings.put("foo", Foo2.class);
		mappings.put("bar", Bar2.class);
		typeMapper.setIdClassMapping(mappings);
		converter.setTypeMapper(typeMapper);
		return converter;
	}

	@Bean
	public NewTopic foos() {
		return new NewTopic("foos", 1, (short) 1);
	}

	@Bean
	public NewTopic bars() {
		return new NewTopic("bars", 1, (short) 1);
	}

}
