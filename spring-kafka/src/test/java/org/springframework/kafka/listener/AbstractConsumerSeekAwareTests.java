package org.springframework.kafka.listener;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.AbstractConsumerSeekAwareTests.Config.MultiGroupListener;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.stereotype.Component;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DirtiesContext
@SpringJUnitConfig
@EmbeddedKafka(topics = {AbstractConsumerSeekAwareTests.TOPIC}, partitions = 3)
public class AbstractConsumerSeekAwareTests {

	static final String TOPIC = "Seek";

	@Autowired
	Config config;

	@Autowired
	KafkaTemplate<String, String> template;

	@Autowired
	MultiGroupListener multiGroupListener;

	@Test
	public void seekForAllGroups() throws Exception {
		template.send(TOPIC, "test-data");
		template.send(TOPIC, "test-data");
		assertTrue(MultiGroupListener.latch1.await(10, TimeUnit.SECONDS));
		assertTrue(MultiGroupListener.latch2.await(10, TimeUnit.SECONDS));

		MultiGroupListener.latch1 = new CountDownLatch(2);
		MultiGroupListener.latch2 = new CountDownLatch(2);

		multiGroupListener.seekToBeginning();
		assertTrue(MultiGroupListener.latch1.await(10, TimeUnit.SECONDS));
		assertTrue(MultiGroupListener.latch2.await(10, TimeUnit.SECONDS));
	}

	@Test
	public void seekForSpecificGroup() throws Exception {
		template.send(TOPIC, "test-data");
		template.send(TOPIC, "test-data");
		assertTrue(MultiGroupListener.latch1.await(10, TimeUnit.SECONDS));
		assertTrue(MultiGroupListener.latch2.await(10, TimeUnit.SECONDS));

		MultiGroupListener.latch1 = new CountDownLatch(2);
		MultiGroupListener.latch2 = new CountDownLatch(2);

		multiGroupListener.seekToBeginningForGroup("group2");
		assertThat(MultiGroupListener.latch1.getCount()).isEqualTo(2);
		assertTrue(MultiGroupListener.latch2.await(10, TimeUnit.SECONDS));
	}

	@EnableKafka
	@Configuration
	static class Config {

		@Autowired
		EmbeddedKafkaBroker broker;

		@Bean
		public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
				ConsumerFactory<String, String> consumerFactory) {
			ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
			factory.setConsumerFactory(consumerFactory);
			return factory;
		}

		@Bean
		ConsumerFactory<String, String> consumerFactory() {
			return new DefaultKafkaConsumerFactory<>(KafkaTestUtils.consumerProps("test-group", "false", this.broker));
		}

		@Bean
		ProducerFactory<String, String> producerFactory() {
			return new DefaultKafkaProducerFactory<>(KafkaTestUtils.producerProps(this.broker));
		}

		@Bean
		KafkaTemplate<String, String> template(ProducerFactory<String, String> pf) {
			return new KafkaTemplate<>(pf);
		}

		@Component
		static class MultiGroupListener extends AbstractConsumerSeekAware {

			static CountDownLatch latch1 = new CountDownLatch(2);
			static CountDownLatch latch2 = new CountDownLatch(2);

			@KafkaListener(groupId = "group1", topics = TOPIC)
			void listenForGroup1(String in) {
//				System.out.printf("[group1] in = %s\n", in); // TODO remove
				latch1.countDown();
			}

			@KafkaListener(groupId = "group2", topics = TOPIC)
			void listenForGroup2(String in) {
//				System.out.printf("[group2] in = %s\n", in); // TODO remove
				latch2.countDown();
			}

			public void seekToBeginningForGroup(String groupIdForSeek) {
				getCallbacksAndTopics().forEach((cb, topics) -> {
					if (groupIdForSeek.equals(cb.getGroupId())) {
						topics.forEach(tp -> cb.seekToBeginning(tp.topic(), tp.partition()));
					}
				});
			}
		}
	}

}
