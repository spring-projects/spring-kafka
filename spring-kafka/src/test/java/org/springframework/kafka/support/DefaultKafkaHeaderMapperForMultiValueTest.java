/*
 * Copyright 2025 the original author or authors.
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

package org.springframework.kafka.support;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerConfigUtils;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.retrytopic.RetryTopicConfiguration;
import org.springframework.kafka.retrytopic.RetryTopicConfigurationBuilder;
import org.springframework.kafka.retrytopic.RetryTopicConfigurationSupport;
import org.springframework.kafka.retrytopic.RetryTopicHeaders;
import org.springframework.kafka.support.DefaultKafkaHeaderMapperForMultiValueTest.Config.MultiValueTestListener;
import org.springframework.kafka.support.DefaultKafkaHeaderMapperForMultiValueTest.RetryTopicConfigurations.FirstTopicListener;
import org.springframework.kafka.support.DefaultKafkaHeaderMapperForMultiValueTest.RetryTopicConfigurations.MyCustomDltProcessor;
import org.springframework.kafka.support.converter.MessagingMessageConverter;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Component;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import static org.assertj.core.api.Assertions.assertThat;

/**
 *
 * @author Sanghyeok An
 *
 * @since 4.0.0
 */

@SpringJUnitConfig
@DirtiesContext
@EmbeddedKafka(partitions = 1, topics = {
		DefaultKafkaHeaderMapperForMultiValueTest.TEST_TOPIC,
		DefaultKafkaHeaderMapperForMultiValueTest.RETRY_TOPIC
})
class DefaultKafkaHeaderMapperForMultiValueTest {

	public final static String TEST_TOPIC = "multi-value.tests";

	public final static String RETRY_TOPIC = "multi-value-retry.tests";

	static final String MULTI_VALUE_HEADER1 = "test-multi-value1";

	static final String MULTI_VALUE_HEADER2 = "test-multi-value2";

	static final String SINGLE_VALUE_HEADER = "test-single-value";

	static final List<String> SHOULD_BE_SINGLE_VALUE_IN_NOT_RETRY = List.of(KafkaHeaders.OFFSET,
																			KafkaHeaders.CONSUMER,
																			KafkaHeaders.TIMESTAMP_TYPE,
																			KafkaHeaders.RECEIVED_PARTITION,
																			KafkaHeaders.RECEIVED_KEY,
																			KafkaHeaders.RECEIVED_TOPIC,
																			KafkaHeaders.RECEIVED_TIMESTAMP,
																			KafkaHeaders.GROUP_ID);

	static final List<String> SHOULD_BE_SINGLE_VALUE_IN_RETRY = List.of(KafkaHeaders.ORIGINAL_OFFSET,
																		KafkaHeaders.ORIGINAL_TIMESTAMP_TYPE,
																		KafkaHeaders.DLT_ORIGINAL_CONSUMER_GROUP,
																		KafkaHeaders.ORIGINAL_TOPIC,
																		KafkaHeaders.ORIGINAL_TIMESTAMP,
																		KafkaHeaders.ORIGINAL_PARTITION,
																		RetryTopicHeaders.DEFAULT_HEADER_ORIGINAL_TIMESTAMP,
																		KafkaHeaders.EXCEPTION_CAUSE_FQCN,
																		KafkaHeaders.EXCEPTION_STACKTRACE,
																		KafkaHeaders.EXCEPTION_FQCN,
																		KafkaHeaders.EXCEPTION_MESSAGE,
																		RetryTopicHeaders.DEFAULT_HEADER_ATTEMPTS,
																		RetryTopicHeaders.DEFAULT_HEADER_BACKOFF_TIMESTAMP);

	@Autowired
	private KafkaTemplate<Integer, String> template;

	@Autowired
	private MultiValueTestListener multiValueTestListener;

	@Autowired
	private FirstTopicListener firstTopicListener;

	@Autowired
	private MyCustomDltProcessor myCustomDltProcessor;

	@Test
	void testForCommonCase() throws InterruptedException {

		// GIVEN
		Iterable<org.apache.kafka.common.header.Header> recordHeaders = List.of(
				new RecordHeader(MULTI_VALUE_HEADER1, "value1".getBytes(StandardCharsets.UTF_8)),
				new RecordHeader(MULTI_VALUE_HEADER1, "value2".getBytes(StandardCharsets.UTF_8)),
				new RecordHeader(MULTI_VALUE_HEADER2, "value3".getBytes(StandardCharsets.UTF_8)),
				new RecordHeader(SINGLE_VALUE_HEADER, "value3".getBytes(StandardCharsets.UTF_8))
		);

		ProducerRecord<Integer, String> record = new ProducerRecord<>(
				TEST_TOPIC, 0, 1, "hello-value", recordHeaders);

		// WHEN
		this.template.send(record);

		// THEN
		assertThat(this.multiValueTestListener.latch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(this.multiValueTestListener.latchForHeader1.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(this.multiValueTestListener.latchForHeader2.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(this.multiValueTestListener.latchForCustomSingleValueHeaders.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(this.multiValueTestListener.latchForSingleValueHeaders.await(10, TimeUnit.SECONDS)).isTrue();

	}

	@Test
	void testForDltAndRetryCase() throws InterruptedException {

		// GIVEN
		Iterable<org.apache.kafka.common.header.Header> recordHeaders = List.of(
				new RecordHeader(MULTI_VALUE_HEADER1, "value1".getBytes(StandardCharsets.UTF_8)),
				new RecordHeader(MULTI_VALUE_HEADER1, "value2".getBytes(StandardCharsets.UTF_8)),
				new RecordHeader(MULTI_VALUE_HEADER2, "value3".getBytes(StandardCharsets.UTF_8)),
				new RecordHeader(SINGLE_VALUE_HEADER, "value3".getBytes(StandardCharsets.UTF_8))
		);

		ProducerRecord<Integer, String> record = new ProducerRecord<>(
				RETRY_TOPIC, 0, 1, "hello-value", recordHeaders);

		// WHEN
		this.template.send(record);

		// THEN
		assertThat(this.firstTopicListener.latch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(this.firstTopicListener.latchForHeader1.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(this.firstTopicListener.latchForHeader2.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(this.firstTopicListener.latchForSingleValueHeadersInFirstTry.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(this.firstTopicListener.latchForCustomSingleValueHeaders.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(this.firstTopicListener.latchForSingleValueHeadersInRetry.await(10, TimeUnit.SECONDS)).isTrue();

		assertThat(this.myCustomDltProcessor.latch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(this.myCustomDltProcessor.latchForHeader1.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(this.myCustomDltProcessor.latchForHeader2.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(this.myCustomDltProcessor.latchForSingleValueHeadersInFirstTry.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(this.myCustomDltProcessor.latchForCustomSingleValueHeaders.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(this.myCustomDltProcessor.latchForSingleValueHeadersInRetry.await(10, TimeUnit.SECONDS)).isTrue();
	}

	static boolean isHeadersExpected(Map<String, Object> headers, String headerName) {
		Object value = headers.get(headerName);
		if (value == null) {
			return false;
		}

		if (value instanceof Iterable<?>) {
			return false;
		}
		return true;
	}

	@Configuration
	@EnableKafka
	public static class Config {

		@Autowired
		private EmbeddedKafkaBroker broker;

		final CountDownLatch latch = new CountDownLatch(1);

		@Bean(KafkaListenerConfigUtils.KAFKA_LISTENER_ENDPOINT_REGISTRY_BEAN_NAME)
		public KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry() {
			return new KafkaListenerEndpointRegistry();
		}

		@Bean
		public KafkaListenerContainerFactory<?> kafkaListenerContainerFactory() {
			ConcurrentKafkaListenerContainerFactory<Integer, String> factory =
					new ConcurrentKafkaListenerContainerFactory<>();

			// For Test
			DefaultKafkaHeaderMapper headerMapper = new DefaultKafkaHeaderMapper(List.of(MULTI_VALUE_HEADER1, MULTI_VALUE_HEADER2));
			MessagingMessageConverter converter = new MessagingMessageConverter(headerMapper);

			factory.setConsumerFactory(consumerFactory());
			factory.setRecordMessageConverter(converter);
			return factory;
		}

		@Bean
		public DefaultKafkaConsumerFactory<Integer, String> consumerFactory() {
			return new DefaultKafkaConsumerFactory<>(consumerConfigs());
		}

		@Bean
		public Map<String, Object> consumerConfigs() {
			Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(
					this.broker.getBrokersAsString(), "mutliValueGroup", "false");
			return consumerProps;
		}

		@Bean
		public KafkaTemplate<Integer, String> template() {
			return new KafkaTemplate<>(producerFactory());
		}

		@Bean
		public ProducerFactory<Integer, String> producerFactory() {
			return new DefaultKafkaProducerFactory<>(producerConfigs());
		}

		@Bean
		public Map<String, Object> producerConfigs() {
			return KafkaTestUtils.producerProps(this.broker.getBrokersAsString());
		}

		@Component
		@KafkaListener(topics = TEST_TOPIC)
		public static class MultiValueTestListener {

			private final CountDownLatch latch = new CountDownLatch(1);

			private final CountDownLatch latchForHeader1 = new CountDownLatch(1);

			private final CountDownLatch latchForHeader2 = new CountDownLatch(1);

			private final CountDownLatch latchForSingleValueHeaders = new CountDownLatch(8);

			private final CountDownLatch latchForCustomSingleValueHeaders = new CountDownLatch(1);

			@KafkaHandler
			public void listen1(@Header(MULTI_VALUE_HEADER1) List<String> header1,
								@Header(MULTI_VALUE_HEADER2) List<Object> header2,
								@Header(SINGLE_VALUE_HEADER) String header3,
								@Headers Map<String, Object> headers, String message) {
				this.latch.countDown();
				if (!header1.isEmpty()) {
					this.latchForHeader1.countDown();
				}

				if (!header2.isEmpty()) {
					this.latchForHeader2.countDown();
				}

				if (header3 != null) {
					this.latchForCustomSingleValueHeaders.countDown();
				}

				for (String headerName : SHOULD_BE_SINGLE_VALUE_IN_NOT_RETRY) {
					if (isHeadersExpected(headers, headerName)) {
						latchForSingleValueHeaders.countDown();
					}
				}
			}

		}

	}

	@Configuration
	static class RetryTopicConfigurations extends RetryTopicConfigurationSupport {

		private static final String DLT_METHOD_NAME = "processDltMessage";

		@Bean
		RetryTopicConfiguration myRetryTopic(KafkaTemplate<Integer, String> template) {
			return RetryTopicConfigurationBuilder
					.newInstance()
					.fixedBackOff(50)
					.maxAttempts(5)
					.concurrency(1)
					.useSingleTopicForSameIntervals()
					.includeTopic(RETRY_TOPIC)
					.doNotRetryOnDltFailure()
					.dltHandlerMethod("myCustomDltProcessor", DLT_METHOD_NAME)
					.create(template);
		}

		@Bean
		FirstTopicListener firstTopicListener() {
			return new FirstTopicListener();
		}

		@Bean
		MyCustomDltProcessor myCustomDltProcessor() {
			return new MyCustomDltProcessor();
		}

		@Bean
		TaskScheduler sched() {
			return new ThreadPoolTaskScheduler();
		}

		@KafkaListener(id = "firstTopicId", topics = RETRY_TOPIC, concurrency = "2")
		static class FirstTopicListener {

			private final CountDownLatch latch = new CountDownLatch(4);

			private final CountDownLatch latchForHeader1 = new CountDownLatch(4);

			private final CountDownLatch latchForHeader2 = new CountDownLatch(4);

			private final CountDownLatch latchForSingleValueHeadersInFirstTry = new CountDownLatch(32);

			private final CountDownLatch latchForCustomSingleValueHeaders = new CountDownLatch(4);

			private final CountDownLatch latchForSingleValueHeadersInRetry = new CountDownLatch(52);

			@KafkaHandler
			public void listen(
					@Header(MULTI_VALUE_HEADER1) List<String> header1,
					@Header(MULTI_VALUE_HEADER2) List<Object> header2,
					@Header(SINGLE_VALUE_HEADER) String header3,
					@Headers Map<String, Object> headers, String message) {
				this.latch.countDown();
				if (!header1.isEmpty()) {
					this.latchForHeader1.countDown();
				}

				if (!header2.isEmpty()) {
					this.latchForHeader2.countDown();
				}

				if (header3 != null) {
					this.latchForCustomSingleValueHeaders.countDown();
				}


				for (String headerName : SHOULD_BE_SINGLE_VALUE_IN_NOT_RETRY) {
					if (isHeadersExpected(headers, headerName)) {
						latchForSingleValueHeadersInFirstTry.countDown();
					}
				}

				for (String headerName : SHOULD_BE_SINGLE_VALUE_IN_RETRY) {
					if (isHeadersExpected(headers, headerName)) {
						latchForSingleValueHeadersInRetry.countDown();
					}
				}
				throw new RuntimeException("Woooops... in topic " + message);
			}

		}

		static class MyCustomDltProcessor {

			private final CountDownLatch latch = new CountDownLatch(1);

			private final CountDownLatch latchForHeader1 = new CountDownLatch(1);

			private final CountDownLatch latchForHeader2 = new CountDownLatch(1);

			private final CountDownLatch latchForSingleValueHeadersInFirstTry = new CountDownLatch(8);

			private final CountDownLatch latchForCustomSingleValueHeaders = new CountDownLatch(1);

			private final CountDownLatch latchForSingleValueHeadersInRetry = new CountDownLatch(13);

			public void processDltMessage(
					Object message,
					@Header(MULTI_VALUE_HEADER1) List<String> header1,
					@Header(MULTI_VALUE_HEADER2) List<Object> header2,
					@Header(SINGLE_VALUE_HEADER) String header3,
					@Headers Map<String, Object> headers) {

				this.latch.countDown();
				if (!header1.isEmpty()) {
					this.latchForHeader1.countDown();
				}

				if (!header2.isEmpty()) {
					this.latchForHeader2.countDown();
				}

				if (header3 != null) {
					this.latchForCustomSingleValueHeaders.countDown();
				}

				for (String headerName : SHOULD_BE_SINGLE_VALUE_IN_NOT_RETRY) {
					if (isHeadersExpected(headers, headerName)) {
						latchForSingleValueHeadersInFirstTry.countDown();
					}
				}

				for (String headerName : SHOULD_BE_SINGLE_VALUE_IN_RETRY) {
					if (isHeadersExpected(headers, headerName)) {
						latchForSingleValueHeadersInRetry.countDown();
					}
				}
				throw new RuntimeException("Woooops... in topic " + message);
			}

		}

	}

}
