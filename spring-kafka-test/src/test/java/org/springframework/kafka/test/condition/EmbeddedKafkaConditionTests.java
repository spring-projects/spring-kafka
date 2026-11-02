/*
 * Copyright 2019-present the original author or authors.
 */

package org.springframework.kafka.test.condition;

import java.time.Duration;

import org.junit.jupiter.api.Test;

import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Gary Russell
 * @author Michał Padula
 * @author Artem Bilan
 *
 * @since 2.3
 *
 */
@EmbeddedKafka(bootstrapServersProperty = "my.bss.property", count = 2, controlledShutdown = true, partitions = 3,
		adminTimeout = 67)
public class EmbeddedKafkaConditionTests {

	@Test
	public void test(EmbeddedKafkaBroker broker) {
		assertThat(broker.getBrokersAsString()).isNotNull();
		assertThat(KafkaTestUtils.getPropertyValue(broker, "brokerListProperty")).isEqualTo("my.bss.property");
		assertThat(KafkaTestUtils.getPropertyValue(broker, "adminTimeout")).isEqualTo(Duration.ofSeconds(67));
		assertThat(broker.getPartitionsPerTopic()).isEqualTo(3);
	}

	@Test
	public void testResolver(EmbeddedKafkaBroker broker) {
		assertThat(broker).isNotNull();
	}

}
