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

package org.springframework.kafka.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.core.CleanupConfig;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

/**
 * @author Pawel Szymczyk
 * @author Artem Bilan
 * @author Gary Russell
 */
@SpringJUnitConfig
@DirtiesContext
@EmbeddedKafka
@DisabledOnOs(OS.WINDOWS)
public class StreamsBuilderFactoryBeanTests {

	private static final String APPLICATION_ID = "testCleanupStreams";

	private static Path stateStoreDir;

	@BeforeAll
	public static void setup() throws IOException {
		stateStoreDir = Files.createTempDirectory("test-state-dir");
	}

	@Autowired
	private StreamsBuilderFactoryBean streamsBuilderFactoryBean;

	@Test
	public void testCleanupStreams() throws IOException {
		Path stateStore = Files.createDirectory(Paths.get(stateStoreDir.toString(), APPLICATION_ID, "0_0"));
		assertThat(stateStore).exists();
		streamsBuilderFactoryBean.stop();
		assertThat(stateStore).doesNotExist();

		stateStore = Files.createDirectory(Paths.get(stateStoreDir.toString(), APPLICATION_ID, "0_0"));
		assertThat(stateStore).exists();
		streamsBuilderFactoryBean.start();
		assertThat(stateStore).doesNotExist();
	}

	@Configuration
	@EnableKafkaStreams
	public static class KafkaStreamsConfig {

		@Value("${" + EmbeddedKafkaBroker.SPRING_EMBEDDED_KAFKA_BROKERS + "}")
		private String brokerAddresses;

		@Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_BUILDER_BEAN_NAME)
		public StreamsBuilderFactoryBean defaultKafkaStreamsBuilder() {
			return new StreamsBuilderFactoryBean(kStreamsConfigs(), new CleanupConfig(true, true));
		}

		@Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
		public KafkaStreamsConfiguration kStreamsConfigs() {
			Map<String, Object> props = new HashMap<>();
			props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
			props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, this.brokerAddresses);
			props.put(StreamsConfig.STATE_DIR_CONFIG, stateStoreDir.toString());
			return new KafkaStreamsConfiguration(props);
		}

	}

}
