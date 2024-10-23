package org.springframework.kafka.test.context;

import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.test.EmbeddedKafkaZKBroker;
import org.springframework.kafka.test.core.BrokerAddress;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(SpringRunner.class)
@DirtiesContext
@EmbeddedKafka(topics = {"topic.1","topic.2"}, brokerProperties = {"listeners=PLAINTEXT://localhost:9092"}, kraft = false)
public class Gh3585Tests {

	@Autowired
	private EmbeddedKafkaZKBroker kafkaBroker;

	@Test
	public void testEmbeddedKafkaZKBroker() {
		assertThat(this.kafkaBroker.getBrokerAddress(0)).isEqualTo(new BrokerAddress("127.0.0.1", 9092));
	}

}
