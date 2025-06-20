/*
 * Copyright 2024-present the original author or authors.
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

package org.springframework.kafka.config;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.BuiltInDslStoreSuppliers;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Cédric Schaller
 * @author Soby Chacko
 */
@SpringJUnitConfig
@DirtiesContext
@EmbeddedKafka
public class StreamsBuilderFactoryBeanInMemoryStateStoreTests {

	private static Path stateStoreDir;

	@BeforeAll
	static void beforeAll() throws IOException {
		stateStoreDir = Files.createTempDirectory(StreamsBuilderFactoryBeanInMemoryStateStoreTests.class.getSimpleName());
	}

	@Test
	void testStateStoreIsInMemory() {
		// Testing that an in-memory state store is used requires accessing the internal state of StreamsBuilder via reflection
		// Therefore, we check the non-existence of RocksDB files instead
		assertThat(stateStoreDir).isEmptyDirectory();
	}

	@Configuration
	@EnableKafkaStreams
	static class KafkaStreamsConfig {

		@Value("${" + EmbeddedKafkaBroker.SPRING_EMBEDDED_KAFKA_BROKERS + "}")
		private String brokerAddresses;

		@Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
		public KafkaStreamsConfiguration kStreamsConfigWithInMemoryStateStores() {
			Map<String, Object> props = new HashMap<>();
			props.put(StreamsConfig.APPLICATION_ID_CONFIG, "should-be-stored-in-memory");
			props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, this.brokerAddresses);
			props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
			props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
			props.put(StreamsConfig.STATE_DIR_CONFIG, stateStoreDir.toString());
			return new KafkaStreamsConfiguration(props);
		}

		@Bean
		public KTable<?, ?> table(StreamsBuilder builder) {
			KStream<Object, Object> stream = builder.stream("source-topic");
			return stream
					.groupByKey()
					.count(Materialized.<Object, Long, KeyValueStore<Bytes, byte[]>>as("store")
							.withStoreType(BuiltInDslStoreSuppliers.IN_MEMORY));
		}
	}
}
