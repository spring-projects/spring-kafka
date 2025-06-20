/*
 * Copyright 2016-present the original author or authors.
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
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.EnableKafka
import org.springframework.kafka.annotation.KafkaHandler
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.core.ProducerFactory
import org.springframework.kafka.listener.KafkaListenerErrorHandler
import org.springframework.kafka.support.Acknowledgment
import org.springframework.kafka.test.EmbeddedKafkaBroker
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.kafka.test.utils.KafkaTestUtils
import org.springframework.messaging.handler.annotation.SendTo
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig
import java.time.Duration
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit


/**
 * Kotlin Annotated async return listener tests.
 *
 * @author Wang ZhiYang
 * @author Artem Bilan
 *
 * @since 3.1
 */
@SpringJUnitConfig
@DirtiesContext
@EmbeddedKafka(topics = ["kotlinAsyncTestTopic1", "kotlinAsyncTestTopic2",
		"kotlinAsyncBatchTestTopic1", "kotlinAsyncBatchTestTopic2", "kotlinReplyTopic1"], partitions = 1)
class EnableKafkaKotlinCoroutinesTests {

	@Autowired
	private lateinit var config: Config

	@Autowired
	private lateinit var template: KafkaTemplate<String, String>

	@Test
	fun `test listener`() {
		this.template.send("kotlinAsyncTestTopic1", "foo")
		assertThat(this.config.latch1.await(10, TimeUnit.SECONDS)).isTrue()
		assertThat(this.config.received).isEqualTo("foo")
		await()
			.untilAsserted {
				assertThat(KafkaTestUtils.getPropertyValue(this.config.acknowledgment, "acked"))
					.isEqualTo(java.lang.Boolean.TRUE)
			}
	}

	@Test
	fun `test checkedEx`() {
		this.template.send("kotlinAsyncTestTopic2", "fail")
		assertThat(this.config.latch2.await(10, TimeUnit.SECONDS)).isTrue()
		assertThat(this.config.error).isTrue()
	}

	@Test
	fun `test batch listener`() {
		this.template.send("kotlinAsyncBatchTestTopic1", "foo")
		assertThat(this.config.batchLatch1.await(10, TimeUnit.SECONDS)).isTrue()
		assertThat(this.config.batchReceived).isEqualTo("foo")
	}

	@Test
	fun `test batch checkedEx`() {
		this.template.send("kotlinAsyncBatchTestTopic2", "fail")
		assertThat(this.config.batchLatch2.await(10, TimeUnit.SECONDS)).isTrue()
		assertThat(this.config.batchError).isTrue()
	}

	@Test
	fun `test checkedKh reply`() {
		this.template.send("kotlinAsyncTestTopic3", "foo")
		val cr = this.template.receive("kotlinReplyTopic1", 0, 0, Duration.ofSeconds(30))
		assertThat(cr?.value() ?: "null").isEqualTo("FOO")
	}

	@KafkaListener(id = "sendTopic", topics = ["kotlinAsyncTestTopic3"],
			containerFactory = "kafkaListenerContainerFactory")
	class Listener {

		@KafkaHandler
		@SendTo("kotlinReplyTopic1")
		suspend fun handler1(value: String) : String {
			return value.uppercase()
		}

	}

	@Configuration
	@EnableKafka
	class Config {

		@Volatile
		lateinit var received: String

		lateinit var acknowledgment: Acknowledgment

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
		fun listener() : Listener {
			return Listener()
		}

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
			val kafkaTemplate = KafkaTemplate(kpf())
			kafkaTemplate.setConsumerFactory(kcf())
			return kafkaTemplate
		}

		@Bean
		fun errorHandler() : KafkaListenerErrorHandler {
			return KafkaListenerErrorHandler { message, _ ->
				error = true;
				latch2.countDown()
				message.payload;
			}
		}

		@Bean
		fun errorHandlerBatch() : KafkaListenerErrorHandler {
			return KafkaListenerErrorHandler { message, _ ->
				batchError = true;
				batchLatch2.countDown()
				message.payload;
			}
		}

		@Bean
		fun kafkaListenerContainerFactory(): ConcurrentKafkaListenerContainerFactory<String, String> {
			val factory: ConcurrentKafkaListenerContainerFactory<String, String>
				= ConcurrentKafkaListenerContainerFactory()
			factory.setConsumerFactory(kcf())
			factory.setReplyTemplate(kt())
			return factory
		}

		@Bean
		fun kafkaBatchListenerContainerFactory(): ConcurrentKafkaListenerContainerFactory<String, String> {
			val factory: ConcurrentKafkaListenerContainerFactory<String, String>
					= ConcurrentKafkaListenerContainerFactory()
			factory.setBatchListener(true)
			factory.setConsumerFactory(kcf())
			return factory
		}

		@KafkaListener(id = "kotlin", topics = ["kotlinAsyncTestTopic1"],
				containerFactory = "kafkaListenerContainerFactory")
		suspend fun listen(value: String, acknowledgment: Acknowledgment) {
			this.received = value
			this.acknowledgment = acknowledgment
			this.latch1.countDown()
		}

		@KafkaListener(id = "kotlin-ex", topics = ["kotlinAsyncTestTopic2"],
				containerFactory = "kafkaListenerContainerFactory", errorHandler = "errorHandler")
		suspend fun listenEx(value: String) {
			if (value == "fail") {
				throw Exception("checked")
			}
		}

		@KafkaListener(id = "kotlin-batch", topics = ["kotlinAsyncBatchTestTopic1"], containerFactory = "kafkaBatchListenerContainerFactory")
		suspend fun batchListen(values: List<ConsumerRecord<String, String>>) {
			this.batchReceived = values.first().value()
			this.batchLatch1.countDown()
		}

		@KafkaListener(id = "kotlin-batch-ex", topics = ["kotlinAsyncBatchTestTopic2"],
				containerFactory = "kafkaBatchListenerContainerFactory", errorHandler = "errorHandlerBatch")
		suspend fun batchListenEx(values: List<ConsumerRecord<String, String>>) {
			if (values.first().value() == "fail") {
				throw Exception("checked")
			}
		}

	}

}
