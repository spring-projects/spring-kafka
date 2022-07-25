/*
 * Copyright 2022 the original author or authors.
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

package org.springframework.kafka.test.junit;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.platform.engine.discovery.DiscoverySelectors;
import org.junit.platform.launcher.core.LauncherDiscoveryRequestBuilder;
import org.junit.platform.launcher.core.LauncherFactory;
import org.junit.platform.launcher.listeners.SummaryGeneratingListener;

import org.springframework.util.DefaultPropertiesPersister;

/**
 * @author Artem Bilan
 *
 * @since 3.0
 */
public class GlobalEmbeddedKafkaTestExecutionListenerTests {

	@BeforeAll
	static void setup() {
		System.setProperty(GlobalEmbeddedKafkaTestExecutionListener.LISTENER_ENABLED_PROPERTY_NAME, "true");
	}

	@AfterAll
	static void tearDown() {
		System.clearProperty(GlobalEmbeddedKafkaTestExecutionListener.LISTENER_ENABLED_PROPERTY_NAME);
	}

	@AfterEach
	void cleanUp() {
		System.clearProperty(GlobalEmbeddedKafkaTestExecutionListener.BROKER_PROPERTIES_LOCATION_PROPERTY_NAME);
	}

	@Test
	void testGlobalEmbeddedKafkaTestExecutionListener() {
		var discoveryRequest =
				LauncherDiscoveryRequestBuilder.request()
						.selectors(DiscoverySelectors.selectClass(TestClass1.class),
								DiscoverySelectors.selectClass(TestClass2.class))
						.build();

		var summaryGeneratingListener = new SummaryGeneratingListener();
		LauncherFactory.create().execute(discoveryRequest, summaryGeneratingListener);

		var summary = summaryGeneratingListener.getSummary();

		try {
			assertThat(summary.getTestsStartedCount()).isEqualTo(2);
			assertThat(summary.getTestsSucceededCount()).isEqualTo(2);
			assertThat(summary.getTestsFailedCount()).isEqualTo(0);
		}
		catch (Exception ex) {
			summary.printFailuresTo(new PrintWriter(System.out));
			throw ex;
		}
	}

	@Test
	void testGlobalEmbeddedKafkaWithBrokerProperties() throws IOException {
		var brokerProperties = new Properties();
		brokerProperties.setProperty("auto.create.topics.enable", "false");

		var propertiesFile = File.createTempFile("kafka-broker", ".properties");

		try (var outputStream = new BufferedOutputStream(new FileOutputStream(propertiesFile))) {
			new DefaultPropertiesPersister().store(brokerProperties, outputStream, "Last entry");
		}

		System.setProperty(GlobalEmbeddedKafkaTestExecutionListener.BROKER_PROPERTIES_LOCATION_PROPERTY_NAME,
				"file:" + propertiesFile.getAbsolutePath());

		var discoveryRequest =
				LauncherDiscoveryRequestBuilder.request()
						.selectors(DiscoverySelectors.selectClass(TestClass1.class),
								DiscoverySelectors.selectClass(TestClass2.class))
						.build();

		var summaryGeneratingListener = new SummaryGeneratingListener();
		LauncherFactory.create().execute(discoveryRequest, summaryGeneratingListener);

		var summary = summaryGeneratingListener.getSummary();

		try {
			assertThat(summary.getTestsStartedCount()).isEqualTo(2);
			assertThat(summary.getTestsSucceededCount()).isEqualTo(1);
			assertThat(summary.getTestsFailedCount()).isEqualTo(1);
		}
		catch (Exception ex) {
			summary.printFailuresTo(new PrintWriter(System.out));
			throw ex;
		}
	}

	@EnabledIfSystemProperty(named = GlobalEmbeddedKafkaTestExecutionListener.LISTENER_ENABLED_PROPERTY_NAME,
			matches = "true")
	static class TestClass1 {

		@Test
		void testDescribeTopic() throws ExecutionException, InterruptedException, TimeoutException {
			Map<String, Object> adminConfigs =
					Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
							System.getProperty("spring.kafka.bootstrap-servers"));
			try (var admin = AdminClient.create(adminConfigs)) {
				var topicsMap =
						admin.describeTopics(Set.of("topic1", "topic2"))
								.allTopicNames()
								.get(10, TimeUnit.SECONDS);

				assertThat(topicsMap).containsOnlyKeys("topic1", "topic2");
			}
		}

	}

	@EnabledIfSystemProperty(named = GlobalEmbeddedKafkaTestExecutionListener.LISTENER_ENABLED_PROPERTY_NAME,
			matches = "true")
	static class TestClass2 {

		@Test
		void testCannotAutoCreateTopic() throws ExecutionException, InterruptedException, TimeoutException {
			Map<String, Object> producerConfigs = new HashMap<>();
			producerConfigs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
					System.getProperty("spring.kafka.bootstrap-servers"));
			producerConfigs.put(ProducerConfig.RETRIES_CONFIG, 1);
			producerConfigs.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 1);
			producerConfigs.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 1000);

			StringSerializer serializer = new StringSerializer();
			try (var kafkaProducer = new KafkaProducer<>(producerConfigs, serializer, serializer)) {
				var recordMetadata =
						kafkaProducer.send(new ProducerRecord<>("nonExistingTopic", "testValue"))
								.get(10, TimeUnit.SECONDS);

				assertThat(recordMetadata).isNotNull();
			}
		}

	}

}
