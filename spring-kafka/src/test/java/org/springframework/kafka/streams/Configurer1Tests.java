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

package org.springframework.kafka.streams;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.config.StreamsBuilderFactoryBeanConfigurer;
import org.springframework.kafka.config.StreamsBuilderFactoryBeanCustomizer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

/**
 * @author Gary Russell
 * @since 2.7
 *
 */
@SpringJUnitConfig
@DirtiesContext
@EmbeddedKafka(partitions = 1,
		topics = Configurer1Tests.STREAMING_TOPIC1,
		brokerProperties = {
				"auto.create.topics.enable=${topics.autoCreate:false}",
				"delete.topic.enable=${topic.delete:true}" },
		brokerPropertiesLocation = "classpath:/${broker.filename:broker}.properties")
public class Configurer1Tests {

	public static final String STREAMING_TOPIC1 = "Configurer1Tests1";

	@Test
	void appliedInOrder(@Autowired List<Integer> callOrder) {
		assertThat(callOrder).containsExactly(1, 2, 3);
	}

	@Configuration
	@EnableKafkaStreams
	public static class Config {

		@Value("${" + EmbeddedKafkaBroker.SPRING_EMBEDDED_KAFKA_BROKERS + "}")
		private String brokerAddresses;

		@Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
		public KafkaStreamsConfiguration kStreamsConfigs() {
			Map<String, Object> props = new HashMap<>();
			props.put(StreamsConfig.APPLICATION_ID_CONFIG, "configurer1");
			props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, this.brokerAddresses);
			props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());
			props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
			props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
					WallclockTimestampExtractor.class.getName());
			props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "100");
			return new KafkaStreamsConfiguration(props);
		}

		@Bean
		public KStream<Integer, String> kStream(StreamsBuilder kStreamBuilder) {
			KStream<Integer, String> stream = kStreamBuilder.stream(STREAMING_TOPIC1);
			stream.foreach((K, v) -> { });
			return stream;
		}

		@Bean
		List<Integer> callOrder() {
			return new ArrayList<>();
		}

		@Bean
		StreamsBuilderFactoryBeanConfigurer three(List<Integer> callOrder) {
			return new StreamsBuilderFactoryBeanConfigurer() {

				@Override
				public void configure(StreamsBuilderFactoryBean factoryBean) {
					callOrder.add(3);
				}

				@Override
				public int getOrder() {
					return Integer.MAX_VALUE;
				}

			};
		}

		@Bean
		StreamsBuilderFactoryBeanConfigurer one(List<Integer> callOrder) {
			return new StreamsBuilderFactoryBeanConfigurer() {

				@Override
				public void configure(StreamsBuilderFactoryBean factoryBean) {
					callOrder.add(1);
				}

				@Override
				public int getOrder() {
					return Integer.MIN_VALUE;
				}

			};
		}

		@Bean
		StreamsBuilderFactoryBeanConfigurer two(List<Integer> callOrder) {
			return new StreamsBuilderFactoryBeanConfigurer() {

				@Override
				public void configure(StreamsBuilderFactoryBean factoryBean) {
					callOrder.add(2);
				}

			};
		}

		@SuppressWarnings("deprecation")
		@Bean
		public StreamsBuilderFactoryBeanCustomizer wontBeFoundNotUnique(List<Integer> callOrder) {
			return fb -> fb.setStateListener((newState, oldState) -> {
				callOrder.add(4);
			});
		}

	}

}
