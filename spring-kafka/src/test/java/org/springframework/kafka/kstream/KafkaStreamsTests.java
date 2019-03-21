/*
 * Copyright 2017 the original author or authors.
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

package org.springframework.kafka.kstream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.apache.kafka.streams.processor.internals.StreamThread;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KStreamBuilderFactoryBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.util.concurrent.SettableListenableFuture;

/**
 * @author Artem Bilan
 * @author Marius Bogoevici
 * @author Gary Russell
 *
 * @since 1.1.4
 */
@RunWith(SpringRunner.class)
@DirtiesContext
@EmbeddedKafka(partitions = 1,
		topics = {
				KafkaStreamsTests.STREAMING_TOPIC1,
				KafkaStreamsTests.STREAMING_TOPIC2,
				KafkaStreamsTests.FOOS },
		brokerProperties = {
				"auto.create.topics.enable=false",
				"delete.topic.enable=true" })
public class KafkaStreamsTests {

	static final String STREAMING_TOPIC1 = "streamingTopic1";

	static final String STREAMING_TOPIC2 = "streamingTopic2";

	static final String FOOS = "foos";

	@Autowired
	private KafkaTemplate<Integer, String> kafkaTemplate;

	@Autowired
	private SettableListenableFuture<String> resultFuture;

	@Autowired
	private KStreamBuilderFactoryBean kStreamBuilderFactoryBean;

	@Autowired
	private KafkaEmbedded kafkaEmbedded;

	@Test
	public void testKStreams() throws Exception {
		assertThat(this.kafkaEmbedded.getKafkaServer(0).config().autoCreateTopicsEnable()).isFalse();
		assertThat(this.kafkaEmbedded.getKafkaServer(0).config().deleteTopicEnable()).isTrue();

		this.kStreamBuilderFactoryBean.stop();

		CountDownLatch stateLatch = new CountDownLatch(1);

		this.kStreamBuilderFactoryBean.setStateListener((newState, oldState) -> stateLatch.countDown());
		Thread.UncaughtExceptionHandler exceptionHandler = mock(Thread.UncaughtExceptionHandler.class);
		this.kStreamBuilderFactoryBean.setUncaughtExceptionHandler(exceptionHandler);

		this.kStreamBuilderFactoryBean.start();

		String payload = "foo" + UUID.randomUUID().toString();
		String payload2 = "foo" + UUID.randomUUID().toString();

		this.kafkaTemplate.sendDefault(0, payload);
		this.kafkaTemplate.sendDefault(0, payload2);
		this.kafkaTemplate.flush();

		String result = resultFuture.get(600, TimeUnit.SECONDS);

		assertThat(result).isNotNull();

		assertThat(result).isEqualTo(payload.toUpperCase() + payload2.toUpperCase());

		assertThat(stateLatch.await(10, TimeUnit.SECONDS)).isTrue();

		KafkaStreams kafkaStreams = this.kStreamBuilderFactoryBean.getKafkaStreams();

		StreamThread[] threads = KafkaTestUtils.getPropertyValue(kafkaStreams, "threads", StreamThread[].class);
		assertThat(threads).isNotEmpty();
		assertThat(threads[0].getUncaughtExceptionHandler()).isSameAs(exceptionHandler);
	}

	@Configuration
	@EnableKafka
	@EnableKafkaStreams
	public static class KafkaStreamsConfiguration {

		@Value("${" + KafkaEmbedded.SPRING_EMBEDDED_KAFKA_BROKERS + "}")
		private String brokerAddresses;

		@Bean
		public ProducerFactory<Integer, String> producerFactory() {
			return new DefaultKafkaProducerFactory<>(producerConfigs());
		}

		@Bean
		public Map<String, Object> producerConfigs() {
			return KafkaTestUtils.senderProps(this.brokerAddresses);
		}

		@Bean
		public KafkaTemplate<Integer, String> template() {
			KafkaTemplate<Integer, String> kafkaTemplate = new KafkaTemplate<>(producerFactory(), true);
			kafkaTemplate.setDefaultTopic(STREAMING_TOPIC1);
			return kafkaTemplate;
		}

		@Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
		public StreamsConfig kStreamsConfigs() {
			Map<String, Object> props = new HashMap<>();
			props.put(StreamsConfig.APPLICATION_ID_CONFIG, "testStreams");
			props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, this.brokerAddresses);
			props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());
			props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
			props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
					WallclockTimestampExtractor.class.getName());
			props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "100");
			return new StreamsConfig(props);
		}

		@Bean
		public KStream<Integer, String> kStream(KStreamBuilder kStreamBuilder) {
			KStream<Integer, String> stream = kStreamBuilder.stream(STREAMING_TOPIC1);
			stream.mapValues(String::toUpperCase)
					.mapValues(Foo::new)
					.through(Serdes.Integer(), new JsonSerde<Foo>() {

					}, FOOS)
					.mapValues(Foo::getName)
					.groupByKey()
					.reduce((value1, value2) -> value1 + value2, TimeWindows.of(1000), "windowStore")
					.toStream()
					.map((windowedId, value) -> new KeyValue<>(windowedId.key(), value))
					.filter((i, s) -> s.length() > 40).to(STREAMING_TOPIC2);

			stream.print();

			return stream;
		}

		@Bean
		public Map<String, Object> consumerConfigs() {
			Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(this.brokerAddresses, "testGroup", "false");
			consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
			return consumerProps;
		}

		@Bean
		public ConsumerFactory<Integer, String> consumerFactory() {
			return new DefaultKafkaConsumerFactory<>(consumerConfigs());
		}

		@Bean
		public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<Integer, String>> kafkaListenerContainerFactory() {
			ConcurrentKafkaListenerContainerFactory<Integer, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
			factory.setConsumerFactory(consumerFactory());
			return factory;
		}

		@Bean
		public SettableListenableFuture<String> resultFuture() {
			return new SettableListenableFuture<>();
		}

		@KafkaListener(topics = STREAMING_TOPIC2)
		public void listener(String payload) {
			resultFuture().set(payload);
		}

	}

	static class Foo {

		private String name;

		Foo() {
		}

		Foo(String name) {
			this.name = name;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

	}

}
