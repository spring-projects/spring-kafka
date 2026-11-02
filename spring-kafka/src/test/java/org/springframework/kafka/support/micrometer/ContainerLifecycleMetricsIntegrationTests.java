/*
 * Copyright 2026-present the original author or authors.
 */

package org.springframework.kafka.support.micrometer;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Integration tests verifying that a {@link KafkaMessageListenerContainer} records the
 * lifecycle start/stop counters against the application context's {@link MeterRegistry}.
 *
 * @author Soby Chacko
 *
 * @since 4.2.0
 */
@SpringJUnitConfig
@EmbeddedKafka(topics = ContainerLifecycleMetricsIntegrationTests.TOPIC, partitions = 1)
@DirtiesContext
class ContainerLifecycleMetricsIntegrationTests {

	static final String TOPIC = "container.lifecycle.metrics.topic";

	@Test
	void startAndStopCountersAccumulateAcrossRestarts(@Autowired ApplicationContext context,
			@Autowired ConsumerFactory<Integer, String> consumerFactory,
			@Autowired MeterRegistry meterRegistry) {

		ContainerProperties props = new ContainerProperties(TOPIC);
		props.setMessageListener((MessageListener<Integer, String>) (ConsumerRecord<Integer, String> record) -> {
		});
		KafkaMessageListenerContainer<Integer, String> container =
				new KafkaMessageListenerContainer<>(consumerFactory, props);
		container.setApplicationContext(context);
		container.setBeanName("lifecycleMetricsTest");

		container.start();
		await().until(container::isRunning);
		container.stop();
		await().until(() -> !container.isRunning());

		container.start();
		await().until(container::isRunning);
		container.stop();
		await().until(() -> !container.isRunning());

		Counter startCounter = meterRegistry.find("spring.kafka.container.start.count")
				.tag("name", "lifecycleMetricsTest")
				.counter();
		Counter stopCounter = meterRegistry.find("spring.kafka.container.stop.count")
				.tag("name", "lifecycleMetricsTest")
				.counter();

		assertThat(startCounter).isNotNull();
		assertThat(stopCounter).isNotNull();
		assertThat(startCounter.count()).isEqualTo(2.0);
		assertThat(stopCounter.count()).isEqualTo(2.0);
	}

	@Test
	void countersAreNotRegisteredWhenMicrometerDisabled(@Autowired ApplicationContext context,
			@Autowired ConsumerFactory<Integer, String> consumerFactory,
			@Autowired MeterRegistry meterRegistry) {

		ContainerProperties props = new ContainerProperties(TOPIC);
		props.setMicrometerEnabled(false);
		props.setMessageListener((MessageListener<Integer, String>) (ConsumerRecord<Integer, String> record) -> {
		});
		KafkaMessageListenerContainer<Integer, String> container =
				new KafkaMessageListenerContainer<>(consumerFactory, props);
		container.setApplicationContext(context);
		container.setBeanName("lifecycleMetricsDisabledTest");

		container.start();
		await().until(container::isRunning);
		container.stop();
		await().until(() -> !container.isRunning());

		assertThat(meterRegistry.find("spring.kafka.container.start.count")
				.tag("name", "lifecycleMetricsDisabledTest").counter()).isNull();
		assertThat(meterRegistry.find("spring.kafka.container.stop.count")
				.tag("name", "lifecycleMetricsDisabledTest").counter()).isNull();
	}

	@Configuration
	static class Config {

		@Bean
		ConsumerFactory<Integer, String> consumerFactory(EmbeddedKafkaBroker broker) {
			return new DefaultKafkaConsumerFactory<>(
					KafkaTestUtils.consumerProps(broker, "lifecycleMetrics", false));
		}

		@Bean
		MeterRegistry meterRegistry() {
			return new SimpleMeterRegistry();
		}

	}

}
