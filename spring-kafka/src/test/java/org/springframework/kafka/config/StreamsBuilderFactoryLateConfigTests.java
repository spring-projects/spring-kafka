/*
 * Copyright 2018-2019 the original author or authors.
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

import java.util.Properties;

import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

/**
 * @author Soby Chacko
 * @author Artem Bilan
 * @author Gary Russell
 */
@SpringJUnitConfig
@DirtiesContext
@EmbeddedKafka
public class StreamsBuilderFactoryLateConfigTests {

	private static final String APPLICATION_ID = "streamsBuilderFactoryLateConfigTests";

	@Value("${" + EmbeddedKafkaBroker.SPRING_EMBEDDED_KAFKA_BROKERS + "}")
	private String brokerAddresses;

	@Autowired
	private StreamsBuilderFactoryBean streamsBuilderFactoryBean;

	@Test
	public void testStreamBuilderFactoryCannotBeStartedWithoutStreamsConfig() {
		StreamsBuilderFactoryBean fb = new StreamsBuilderFactoryBean();
		assertThatExceptionOfType(KafkaException.class).isThrownBy(() -> fb.start());
	}

	@Test
	public void testStreamBuilderFactoryCannotBeInstantiatedWhenAutoStart() {
		StreamsBuilderFactoryBean fb = new StreamsBuilderFactoryBean();
		fb.setAutoStartup(true);
		assertThatIllegalStateException().isThrownBy(() -> fb.createInstance());
	}

	@Test
	public void testStreamsBuilderFactoryWithConfigProvidedLater() {
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, this.brokerAddresses);
		streamsBuilderFactoryBean.setStreamsConfiguration(props);

		assertThat(streamsBuilderFactoryBean.isRunning()).isFalse();
		streamsBuilderFactoryBean.start();
		assertThat(streamsBuilderFactoryBean.isRunning()).isTrue();
	}

	@Configuration
	@EnableKafka
	@EnableKafkaStreams
	public static class KafkaStreamsConfiguration {

		@Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_BUILDER_BEAN_NAME)
		public StreamsBuilderFactoryBean defaultKafkaStreamsBuilder() {
			StreamsBuilderFactoryBean streamsBuilderFactoryBean = new StreamsBuilderFactoryBean();
			streamsBuilderFactoryBean.setAutoStartup(false);
			return streamsBuilderFactoryBean;
		}

	}

}
