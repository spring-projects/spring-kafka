/*
 * Copyright 2019-present the original author or authors.
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

package org.springframework.kafka.listener;

import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.ContainerPostProcessor;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * Tests for container customizations.
 *
 * @author Francois Rosiere
 * @author Soby Chacko
 * @since 3.1
 */
@SuppressWarnings("unused")
@SpringJUnitConfig
@DirtiesContext
class ContainerCustomizationTests {

	private static final String TOPIC = "foo";

	private static final String DEFAULT_LISTENER = "default-listener";

	private static final String CONTAINER_CUSTOMIZER = "container-customizer";

	private static final String POST_PROCESSOR = "post-processor";

	private static final String POST_PROCESSOR_MULTI_METHOD = "post-processor-multi-method";

	private static final String CONTAINER_CUSTOMIZER_AND_POST_PROCESSOR = "container-customizer" +
			"-and-post-processor";

	@Autowired
	private ListenerContainerRegistry listenerContainerRegistry;

	private static Stream<Arguments> listenerIdsWithRelatedInfo() {
		return Stream.of(
				Arguments.of(DEFAULT_LISTENER, null),
				Arguments.of(CONTAINER_CUSTOMIZER,
						CONTAINER_CUSTOMIZER.getBytes(StandardCharsets.UTF_8)),
				Arguments.of(POST_PROCESSOR, POST_PROCESSOR.getBytes(StandardCharsets.UTF_8)),
				Arguments.of(POST_PROCESSOR_MULTI_METHOD,
						POST_PROCESSOR.getBytes(StandardCharsets.UTF_8)),
				Arguments.of(CONTAINER_CUSTOMIZER_AND_POST_PROCESSOR,
						POST_PROCESSOR.getBytes(StandardCharsets.UTF_8)));
	}

	@ParameterizedTest
	@MethodSource("listenerIdsWithRelatedInfo")
	void testCustomization(String listenerId, byte[] listenerInfo) {
		final var listenerContainer = listenerContainerRegistry.getListenerContainer(listenerId);
		assertThat(listenerContainer).isNotNull();
		assertThat(listenerContainer.getListenerInfo()).isEqualTo(listenerInfo);
	}

	@KafkaListener(id = POST_PROCESSOR_MULTI_METHOD,
			topics = TOPIC,
			containerPostProcessor = "infoContainerPostProcessor"
	)
	static class MultiMethodListener {

		@KafkaHandler
		public void listen(String foo) {
		}

		@KafkaHandler
		public void listen(Integer foo) {
		}
	}

	@Configuration
	@EnableKafka
	static class Config {

		@KafkaListener(
				id = DEFAULT_LISTENER,
				topics = TOPIC)
		public void defaultListener(String foo) {
		}

		@KafkaListener(
				id = CONTAINER_CUSTOMIZER,
				topics = TOPIC,
				containerFactory = "containerFactoryWithCustomizer")
		public void containerCustomizer(String foo) {
		}

		@KafkaListener(
				id = POST_PROCESSOR,
				topics = TOPIC,
				containerPostProcessor = "infoContainerPostProcessor")
		public void postProcessor(String foo) {
		}

		@KafkaListener(
				id = CONTAINER_CUSTOMIZER_AND_POST_PROCESSOR,
				topics = TOPIC,
				containerFactory = "containerFactoryWithCustomizer",
				containerPostProcessor = "#{__listener.infoContainerPostProcessor}")
		public void containerCustomizerAndPostProcessor(String foo) {
		}

		@Bean
		public MultiMethodListener multiMethodListener() {
			return new MultiMethodListener();
		}

		@Bean
		public ContainerPostProcessor<String, String, AbstractMessageListenerContainer<String,
				String>> infoContainerPostProcessor() {
			return container -> container.setListenerInfo(POST_PROCESSOR.getBytes(StandardCharsets.UTF_8));
		}

		@Bean
		@SuppressWarnings("unchecked")
		public ConsumerFactory<String, String> consumerFactory() {
			return mock(ConsumerFactory.class);
		}

		@Bean
		@Primary
		public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
			return createKafkaListenerContainerFactory();
		}

		@Bean
		public ConcurrentKafkaListenerContainerFactory<String, String> containerFactoryWithCustomizer() {
			final var containerFactory = createKafkaListenerContainerFactory();
			containerFactory.setContainerCustomizer(container ->
					container.setListenerInfo(CONTAINER_CUSTOMIZER.getBytes(StandardCharsets.UTF_8))
			);
			return containerFactory;
		}

		private ConcurrentKafkaListenerContainerFactory<String, String> createKafkaListenerContainerFactory() {
			final var factory = new ConcurrentKafkaListenerContainerFactory<String, String>();
			factory.setConsumerFactory(consumerFactory());
			factory.setAutoStartup(false);
			return factory;
		}
	}
}
