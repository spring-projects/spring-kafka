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
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import org.springframework.kafka.listener.DefaultErrorHandler
import org.springframework.util.backoff.FixedBackOff


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
		"kotlinAsyncBatchTestTopic1", "kotlinAsyncBatchTestTopic2", "kotlinReplyTopic1",
		"kotlinAsyncTestTopicCommonHandler", "kotlinAsyncTestTopicBoundedRetry",
		"kotlinAsyncTestTopicTwoRecordRetry"], partitions = 1)
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

	@Test
	fun `test suspend function with CommonErrorHandler`() {
		this.template.send("kotlinAsyncTestTopicCommonHandler", "fail")
		assertThat(this.config.commonHandlerLatch.await(10, TimeUnit.SECONDS)).isTrue()
	}

	@Test
	fun `test suspend function bounded retries with CommonErrorHandler`() {
		// GH-4465: an always-failing suspend @KafkaListener with
		// DefaultErrorHandler(FixedBackOff(interval, n)) must be delivered exactly
		// n + 1 times (initial delivery + n retries) and then stop, matching the
		// behaviour of a blocking listener with the same configuration.
		//
		// The recovered latch only signals the *first* recovery, so under the bug
		// (#4465) it still trips while subsequent cycles keep re-delivering the
		// record. The actual regression check is the delivery counter, which must
		// reach 3 and stay there.
		this.template.send("kotlinAsyncTestTopicBoundedRetry", "fail")
		assertThat(this.config.boundedRetryRecoveredLatch.await(10, TimeUnit.SECONDS)).isTrue()
		await()
			.pollDelay(Duration.ofSeconds(2))
			.atMost(Duration.ofSeconds(3))
			.untilAsserted {
				assertThat(this.config.boundedRetryDeliveries.get()).isEqualTo(3)
			}
	}

	@Test
	fun `test suspend function bounded retries for two records on the same partition`() {
		// GH-4504: With two always-failing records on the same partition delivered
		// to a suspend @KafkaListener with DefaultErrorHandler(FixedBackOff), the
		// earlier-offset record was silently skipped: per-record async failure
		// handling registered a seek to the first failed offset, which was then
		// clobbered by the second record's seek before the next poll. The first
		// record was never re-delivered, never reached the recoverer, and the
		// committed offset advanced past it after the second record's recovery.
		//
		// After the fix both records reach the recoverer exactly once and the
		// earlier-offset record is delivered the expected number of times. The
		// later-offset record receives extra deliveries during the earlier
		// record's retry cycle because the async dispatch model invokes the
		// listener on every polled record before any async failure callback
		// fires; the count is still bounded.
		this.template.send("kotlinAsyncTestTopicTwoRecordRetry", "r1")
		this.template.send("kotlinAsyncTestTopicTwoRecordRetry", "r2")
		// Both records must reach the recoverer, not just the later one.
		assertThat(this.config.twoRecordRetryRecoveredLatch.await(15, TimeUnit.SECONDS)).isTrue()
		await()
			.pollDelay(Duration.ofSeconds(2))
			.atMost(Duration.ofSeconds(3))
			.untilAsserted {
				assertThat(this.config.twoRecordRetryRecoveries["r1"]).isEqualTo(1)
				assertThat(this.config.twoRecordRetryRecoveries["r2"]).isEqualTo(1)
				// The earlier-offset record is retried exactly n + 1 times.
				assertThat(this.config.twoRecordRetryDeliveries["r1"]).isEqualTo(3)
				// The later-offset record's delivery count depends on poll
				// timing: 3 if r1 finishes before r2 is ever polled (matches
				// the blocking listener), up to 5 if r1 and r2 share the first
				// poll and r2 is re-fetched alongside r1 on every retry cycle.
				// The real proof of the fix is the recoveries assertion above
				// (both records reach the recoverer exactly once); bound the
				// delivery count to the same window the blocking listener could
				// see plus the two incidental re-deliveries from the async
				// dispatch model.
				assertThat(this.config.twoRecordRetryDeliveries["r2"]).isBetween(3, 5)
			}
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

		val commonHandlerLatch = CountDownLatch(1)

		val boundedRetryDeliveries = AtomicInteger()

		val boundedRetryRecoveredLatch = CountDownLatch(1)

		val twoRecordRetryDeliveries = ConcurrentHashMap<String, Int>()

		val twoRecordRetryRecoveries = ConcurrentHashMap<String, Int>()

		// One countdown per record so the test waits until BOTH records have been recovered.
		val twoRecordRetryRecoveredLatch = CountDownLatch(2)

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

		@Bean
		fun commonErrorHandler(): DefaultErrorHandler {
			return DefaultErrorHandler { record, exception ->
				commonHandlerLatch.countDown()
			}
		}

		@Bean
		fun kafkaListenerContainerFactoryWithCommonHandler(): ConcurrentKafkaListenerContainerFactory<String, String> {
			val factory: ConcurrentKafkaListenerContainerFactory<String, String>
					= ConcurrentKafkaListenerContainerFactory()
			factory.setConsumerFactory(kcf())
			factory.setCommonErrorHandler(commonErrorHandler())
			return factory
		}

		@Bean
		fun boundedRetryErrorHandler(): DefaultErrorHandler {
			return DefaultErrorHandler({ _, _ -> boundedRetryRecoveredLatch.countDown() },
					FixedBackOff(100L, 2L))
		}

		@Bean
		fun kafkaListenerContainerFactoryWithBoundedRetry(): ConcurrentKafkaListenerContainerFactory<String, String> {
			val factory: ConcurrentKafkaListenerContainerFactory<String, String>
					= ConcurrentKafkaListenerContainerFactory()
			factory.setConsumerFactory(kcf())
			factory.setCommonErrorHandler(boundedRetryErrorHandler())
			return factory
		}

		@Bean
		fun twoRecordRetryErrorHandler(): DefaultErrorHandler {
			return DefaultErrorHandler({ record, _ ->
				twoRecordRetryRecoveries.merge(record.value() as String, 1, Int::plus)
				twoRecordRetryRecoveredLatch.countDown()
			}, FixedBackOff(100L, 2L))
		}

		@Bean
		fun kafkaListenerContainerFactoryWithTwoRecordRetry(): ConcurrentKafkaListenerContainerFactory<String, String> {
			val factory: ConcurrentKafkaListenerContainerFactory<String, String>
					= ConcurrentKafkaListenerContainerFactory()
			factory.setConsumerFactory(kcf())
			factory.setCommonErrorHandler(twoRecordRetryErrorHandler())
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

		@KafkaListener(id = "kotlin-common-handler", topics = ["kotlinAsyncTestTopicCommonHandler"],
				containerFactory = "kafkaListenerContainerFactoryWithCommonHandler")
		suspend fun listenWithCommonHandler(value: String) {
			if (value == "fail") {
				throw RuntimeException("Test exception for CommonErrorHandler")
			}
		}

		@KafkaListener(id = "kotlin-bounded-retry", topics = ["kotlinAsyncTestTopicBoundedRetry"],
				containerFactory = "kafkaListenerContainerFactoryWithBoundedRetry")
		suspend fun listenBoundedRetry(value: String) {
			boundedRetryDeliveries.incrementAndGet()
			throw RuntimeException("Always fail (bounded retry)")
		}

		@KafkaListener(id = "kotlin-two-record-retry", topics = ["kotlinAsyncTestTopicTwoRecordRetry"],
				containerFactory = "kafkaListenerContainerFactoryWithTwoRecordRetry")
		suspend fun listenTwoRecordRetry(value: String) {
			twoRecordRetryDeliveries.merge(value, 1, Int::plus)
			throw RuntimeException("Always fail (two-record retry)")
		}

	}

}
