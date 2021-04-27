/*
 * Copyright 2016-2021 the original author or authors.
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
import org.springframework.kafka.listener.BatchErrorHandler
import org.springframework.kafka.listener.BatchMessageListener
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer
import org.springframework.kafka.listener.ErrorHandler
import org.springframework.kafka.listener.MessageListener
import org.springframework.kafka.test.EmbeddedKafkaBroker
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig
import java.lang.Exception
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit


/**
 * @author Gary Russell
 * @since 2.2
 */

@SpringJUnitConfig
@DirtiesContext
@EmbeddedKafka(topics = ["kotlinTestTopic1", "kotlinBatchTestTopic1", "kotlinTestTopic2", "kotlinBatchTestTopic2"])
class EnableKafkaKotlinTests {

	@Autowired
	private lateinit var config: Config

	@Autowired
	private lateinit var template: KafkaTemplate<String, String>

	@Test
	fun `test listener`() {
		this.template.send("kotlinTestTopic1", "foo")
		assertThat(this.config.latch1.await(10, TimeUnit.SECONDS)).isTrue()
		assertThat(this.config.received).isEqualTo("foo")
	}

	@Test
	fun `test checkedEx`() {
		this.template.send("kotlinTestTopic2", "fail")
		assertThat(this.config.latch2.await(10, TimeUnit.SECONDS)).isTrue()
		assertThat(this.config.error).isTrue()
	}

	@Test
	fun `test batch listener`() {
		this.template.send("kotlinBatchTestTopic1", "foo")
		assertThat(this.config.batchLatch1.await(10, TimeUnit.SECONDS)).isTrue()
		assertThat(this.config.batchReceived).isEqualTo("foo")
	}

	@Test
	fun `test batch checkedEx`() {
		this.template.send("kotlinBatchTestTopic2", "fail")
		assertThat(this.config.batchLatch2.await(10, TimeUnit.SECONDS)).isTrue()
		assertThat(this.config.batchError).isTrue()
	}

	@Configuration
	@EnableKafka
	class Config {

		@Volatile
		lateinit var received: String

		@Volatile
		lateinit var batchReceived: String

		@Volatile
		var error: Boolean = false

		@Volatile
		var batchError: Boolean = false

		val latch1 = CountDownLatch(1)

		val latch2 = CountDownLatch(1)

		val batchLatch1 = CountDownLatch(1)

		val batchLatch2 = CountDownLatch(1)

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

		val eh = ErrorHandler { _, recs : ConsumerRecord<*, *>? ->
			if (recs != null) {
				this.error = true;
				this.latch2.countDown()
			}
		}

		@Bean
		fun kafkaListenerContainerFactory(): ConcurrentKafkaListenerContainerFactory<String, String> {
			val factory: ConcurrentKafkaListenerContainerFactory<String, String>
				= ConcurrentKafkaListenerContainerFactory()
			factory.consumerFactory = kcf()
			factory.setErrorHandler(eh)
			return factory
		}

		val beh = BatchErrorHandler { _, recs ->
			if (!recs.isEmpty) {
				this.batchError = true;
				this.batchLatch2.countDown()
			}
		}

		@Bean
		fun kafkaBatchListenerContainerFactory(): ConcurrentKafkaListenerContainerFactory<String, String> {
			val factory: ConcurrentKafkaListenerContainerFactory<String, String>
					= ConcurrentKafkaListenerContainerFactory()
			factory.isBatchListener = true
			factory.consumerFactory = kcf()
			factory.setBatchErrorHandler(beh)
			return factory
		}

		@KafkaListener(id = "kotlin", topics = ["kotlinTestTopic1"], containerFactory = "kafkaListenerContainerFactory")
		fun listen(value: String) {
			this.received = value
			this.latch1.countDown()
		}

		@KafkaListener(id = "kotlin-batch", topics = ["kotlinBatchTestTopic1"], containerFactory = "kafkaBatchListenerContainerFactory")
		fun batchListen(values: List<ConsumerRecord<String, String>>) {
			this.batchReceived = values.first().value()
			this.batchLatch1.countDown()
		}

		@Bean
		fun checkedEx(kafkaListenerContainerFactory : ConcurrentKafkaListenerContainerFactory<String, String>) :
				ConcurrentMessageListenerContainer<String, String> {

			val container = kafkaListenerContainerFactory.createContainer("kotlinTestTopic2")
			container.containerProperties.groupId = "checkedEx"
			container.containerProperties.messageListener = MessageListener<String, String> {
				if (it.value() == "fail") {
					throw Exception("checked")
				}
			}
			return container;
		}

		@Bean
		fun batchCheckedEx(kafkaBatchListenerContainerFactory :
						   ConcurrentKafkaListenerContainerFactory<String, String>) :
				ConcurrentMessageListenerContainer<String, String> {

			val container = kafkaBatchListenerContainerFactory.createContainer("kotlinBatchTestTopic2")
			container.containerProperties.groupId = "batchCheckedEx"
			container.containerProperties.messageListener = BatchMessageListener<String, String> {
				if (it.first().value() == "fail") {
					throw Exception("checked")
				}
			}
			return container;
		}

	}

}
