/*
 * Copyright 2016-2018 the original author or authors.
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

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.EnableKafka
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.core.ProducerFactory
import org.springframework.kafka.test.EmbeddedKafkaBroker
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit


/**
 * @author Gary Russell
 * @since 2.2
 */

@SpringJUnitConfig
@DirtiesContext
@EmbeddedKafka(topics = ["kotlinTestTopic"])
class EnableKafkaKotlinTests {

	@Autowired
	private lateinit var config: Config

	@Autowired
	private lateinit var template: KafkaTemplate<String, String>

	@Test
	fun `test listener`() {
		this.template.send("kotlinTestTopic", "foo")
		assertThat(this.config.latch.await(10, TimeUnit.SECONDS)).isTrue()
		assertThat(this.config.received).isEqualTo("foo")
	}

	@Test
	fun `test batch listener`() {
		this.template.send("kotlinTestTopic", "foo")
		assertThat(this.config.latch.await(10, TimeUnit.SECONDS)).isTrue()
		assertThat(this.config.batchReceived).isEqualTo("foo")
	}

	@Configuration
	@EnableKafka
	class Config {

		lateinit var received: String
		lateinit var batchReceived: String

		val latch = CountDownLatch(2)

		@Value("\${" + EmbeddedKafkaBroker.SPRING_EMBEDDED_KAFKA_BROKERS + "}")
		private lateinit var brokerAddresses: String

		@Bean
		fun kpf(): ProducerFactory<String, String> {
			val configs = HashMap<String, Any>()
			configs[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = this.brokerAddresses
			configs[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
			configs[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
			return DefaultKafkaProducerFactory(configs)
		}

		@Bean
		fun kcf(): ConsumerFactory<String, String> {
			val configs = HashMap<String, Any>()
			configs[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = this.brokerAddresses
			configs[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
			configs[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
			configs[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG] = false
			configs[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"
			return DefaultKafkaConsumerFactory(configs)
		}

		@Bean
		fun kt(): KafkaTemplate<String, String> {
			return KafkaTemplate(kpf())
		}

		@Bean
		fun kafkaListenerContainerFactory(): ConcurrentKafkaListenerContainerFactory<String, String> {
			val factory: ConcurrentKafkaListenerContainerFactory<String, String>
				= ConcurrentKafkaListenerContainerFactory()
			factory.consumerFactory = kcf()
			return factory
		}

		@Bean
		fun kafkaBatchListenerContainerFactory(): ConcurrentKafkaListenerContainerFactory<String, String> {
			val factory: ConcurrentKafkaListenerContainerFactory<String, String>
					= ConcurrentKafkaListenerContainerFactory()
			factory.isBatchListener = true
			factory.consumerFactory = kcf()
			return factory
		}

		@KafkaListener(id = "kotlin", topics = ["kotlinTestTopic"], containerFactory = "kafkaListenerContainerFactory")
		fun listen(value: String) {
			this.received = value
			this.latch.countDown()
		}

		@KafkaListener(id = "kotlin-batch", topics = ["kotlinTestTopic"], containerFactory = "kafkaBatchListenerContainerFactory")
		fun batchListen(values: List<ConsumerRecord<String, String>>) {
			this.batchReceived = values.first().value()
			this.latch.countDown()
		}

	}

}
