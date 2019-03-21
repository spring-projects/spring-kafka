/*
 * Copyright 2018 the original author or authors.
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

package org.springframework.kafka.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;


/**
 * @author Nurettin Yilmaz
 *
 * @since 2.1.5
 */
@SpringJUnitConfig
@DirtiesContext
@EmbeddedKafka
public class KafkaStreamsCustomizerTests {

	private static final String APPLICATION_ID = "testStreams";

	private static final TestStateListener STATE_LISTENER = new TestStateListener();

	@Autowired
	private StreamsBuilderFactoryBean streamsBuilderFactoryBean;

	@Test
	public void testKafkaStreamsCustomizer() {
		KafkaStreams.State state = this.streamsBuilderFactoryBean.getKafkaStreams().state();
		assertThat(STATE_LISTENER.getCurrentState()).isEqualTo(state);
	}

	@Configuration
	@EnableKafka
	@EnableKafkaStreams
	public static class KafkaStreamsConfiguration {

		@Value("${" + KafkaEmbedded.SPRING_EMBEDDED_KAFKA_BROKERS + "}")
		private String brokerAddresses;

		@Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_BUILDER_BEAN_NAME)
		public StreamsBuilderFactoryBean defaultKafkaStreamsBuilder() {
			StreamsBuilderFactoryBean streamsBuilderFactoryBean = new StreamsBuilderFactoryBean(kStreamsConfigs());
			streamsBuilderFactoryBean.setKafkaStreamsCustomizer(customizer());
			return streamsBuilderFactoryBean;
		}

		@Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
		public StreamsConfig kStreamsConfigs() {
			Map<String, Object> props = new HashMap<>();
			props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
			props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, this.brokerAddresses);
			return new StreamsConfig(props);
		}


		private KafkaStreamsCustomizer customizer() {
			return kafkaStreams -> kafkaStreams.setStateListener(STATE_LISTENER);
		}

	}

	static class TestStateListener implements KafkaStreams.StateListener {

		private KafkaStreams.State currentState;

		@Override
		public void onChange(KafkaStreams.State newState, KafkaStreams.State oldState) {
			this.currentState = newState;
		}

		KafkaStreams.State getCurrentState() {
			return this.currentState;
		}

	}

}
