/*
 * Copyright 2021 the original author or authors.
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

package org.springframework.kafka.listener.adapter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

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
import org.springframework.kafka.listener.BatchListenerFailedException;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.kafka.listener.ListenerUtils;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.converter.BatchMessagingMessageConverter;
import org.springframework.kafka.support.converter.ConversionException;
import org.springframework.kafka.support.converter.JsonMessageConverter;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

/**
 * @author Gary Russell
 * @since 2.8
 *
 */
@SpringJUnitConfig
@DirtiesContext
public class BatchAdapterConversionErrorsTests {

	@SuppressWarnings("unchecked")
	@Test
	void testNullInList(@Autowired KafkaListenerEndpointRegistry registry, @Autowired Listener listener) {
		BatchMessagingMessageListenerAdapter<String, String> adapter =
				(BatchMessagingMessageListenerAdapter<String, String>) registry
					.getListenerContainer("foo").getContainerProperties().getMessageListener();
		ConsumerRecord<String, String> junkRecord = new ConsumerRecord<>("foo", 0, 0L, null, "JUNK");
		assertThatExceptionOfType(ListenerExecutionFailedException.class).isThrownBy(() ->
				adapter.onMessage(List.of(
						new ConsumerRecord<>("foo", 0, 0L, null, "{\"bar\":\"baz\"}"),
						junkRecord,
						new ConsumerRecord<>("foo", 0, 0L, null, "{\"bar\":\"qux\"}")), null, null))
				.withCauseExactlyInstanceOf(BatchListenerFailedException.class)
				.extracting(t -> t.getCause())
				.extracting("index")
				.isEqualTo(1);
		assertThat(listener.values).containsExactly(new Foo("baz"), null, new Foo("qux"));
		DeserializationException vDeserEx = ListenerUtils.getExceptionFromHeader(junkRecord,
				ErrorHandlingDeserializer.VALUE_DESERIALIZER_EXCEPTION_HEADER, null);
		assertThat(vDeserEx).isNotNull();
		assertThat(vDeserEx.getData()).isEqualTo("JUNK".getBytes());
	}

	public static class Listener {

		final List<Foo> values = new ArrayList<>();

		@KafkaListener(id = "foo", topics = "foo", autoStartup = "false")
		public void listen(List<Foo> list,
				@Header(KafkaHeaders.CONVERSION_FAILURES) List<ConversionException> conversionFailures) {

			this.values.addAll(list);
			for (int i = 0; i < list.size(); i++) {
				if (conversionFailures.get(i) != null) {
					throw new BatchListenerFailedException("Conversion Failed", conversionFailures.get(i), i);
				}
			}
		}

	}

	public static class Foo {

		private String bar;

		public Foo() {
		}

		public Foo(String bar) {
			this.bar = bar;
		}

		public String getBar() {
			return this.bar;
		}

		public void setBar(String bar) {
			this.bar = bar;
		}

		@Override
		public int hashCode() {
			return Objects.hash(bar);
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj) {
				return true;
			}
			if (obj == null) {
				return false;
			}
			if (getClass() != obj.getClass()) {
				return false;
			}
			Foo other = (Foo) obj;
			return Objects.equals(this.bar, other.bar);
		}

		@Override
		public String toString() {
			return "Foo [bar=" + this.bar + "]";
		}

	}

	@Configuration
	@EnableKafka
	public static class Config {

		@Bean
		public Listener foo() {
			return new Listener();
		}

		@SuppressWarnings({ "rawtypes" })
		@Bean
		public ConsumerFactory consumerFactory() {
			ConsumerFactory consumerFactory = mock(ConsumerFactory.class);
			return consumerFactory;
		}

		@SuppressWarnings({ "rawtypes", "unchecked" })
		@Bean
		public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
			ConcurrentKafkaListenerContainerFactory factory = new ConcurrentKafkaListenerContainerFactory();
			factory.setConsumerFactory(consumerFactory());
			factory.setMessageConverter(new BatchMessagingMessageConverter(new JsonMessageConverter()));
			factory.setBatchListener(true);
			return factory;
		}
	}

}
