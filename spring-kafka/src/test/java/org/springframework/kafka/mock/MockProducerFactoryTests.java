package org.springframework.kafka.mock;

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Pawel Szymczyk
 */
public class MockProducerFactoryTests {

	@Test
	void testSendingMultipleMessagesWithMockProducer() {
		MockProducer<String, String> mockProducer = new MockProducer<>(true, new StringSerializer(), new StringSerializer());
		MockProducerFactory<String, String> mockProducerFactory = new MockProducerFactory<>(() -> mockProducer);
		KafkaTemplate<String, String> kafkaTemplate = new KafkaTemplate<>(mockProducerFactory);
		kafkaTemplate.send("topic", "Hello");
		kafkaTemplate.send("topic", "World");
		assertThat(mockProducer.history()).hasSize(2);
	}
}
