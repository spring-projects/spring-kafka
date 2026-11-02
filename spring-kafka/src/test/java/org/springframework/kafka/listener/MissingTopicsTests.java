/*
 * Copyright 2018-present the original author or authors.
 */

package org.springframework.kafka.listener;

import java.util.Map;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.condition.EmbeddedKafkaCondition;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

/**
 * @author Gary Russell
 * @author Ngoc Nhan
 * @since 2.2
 *
 */
@EmbeddedKafka(brokerProperties = "auto.create.topics.enable=false")
public class MissingTopicsTests {

	private static EmbeddedKafkaBroker embeddedKafka;

	@BeforeAll
	public static void setup() {
		embeddedKafka = EmbeddedKafkaCondition.getBroker();
	}

	@Test
	public void testMissingTopicCMLC() {
		Map<String, Object> props = KafkaTestUtils.consumerProps(embeddedKafka, "missing1", true);
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(props);
		ContainerProperties containerProps = new ContainerProperties("notexisting");
		containerProps.setMessageListener((MessageListener<Integer, String>) message -> { });
		containerProps.setMissingTopicsFatal(true);
		ConcurrentMessageListenerContainer<Integer, String> container =
				new ConcurrentMessageListenerContainer<>(cf, containerProps);
		container.setBeanName("testMissing1");
		assertThatIllegalStateException().isThrownBy(container::start)
				.withMessageContaining("missingTopicsFatal");
	}

	@Test
	public void testMissingTopicKMLC() {
		Map<String, Object> props = KafkaTestUtils.consumerProps(embeddedKafka, "missing2", true);
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(props);
		ContainerProperties containerProps = new ContainerProperties("notexisting");
		containerProps.setMessageListener((MessageListener<Integer, String>) message -> { });
		containerProps.setMissingTopicsFatal(true);
		KafkaMessageListenerContainer<Integer, String> container =
				new KafkaMessageListenerContainer<>(cf, containerProps);
		container.setBeanName("testMissing2");
		assertThatIllegalStateException().isThrownBy(container::start)
				.withMessageContaining("missingTopicsFatal");
		container.getContainerProperties().setMissingTopicsFatal(false);
		container.start();
		container.stop();
	}

}
