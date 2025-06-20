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

package org.springframework.kafka.core

import org.apache.kafka.clients.producer.MockProducer
import org.apache.kafka.clients.producer.Producer
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

/**
 * @author Antonio Tomac
 * @since 2.9.6
 * @see [ProducerFactory.createProducer]
 */
class KotlinProducerFactoryTests {

	class KotlinProducerFactory : ProducerFactory<Any, Any> {
		override fun createProducer(): Producer<Any, Any> = MockProducer()

		/**
		 * This override checks correct nullability of [txIdPrefix] in [ProducerFactory.createProducer]
		 * Reason, kotlin compiler warning:
		 *
		 * Override 'fun createProducer(txIdPrefix: String?): Producer<Any, Any>' has incorrect nullability
		 * in its signature comparing with overridden 'fun createProducer(txIdPrefix: String): Producer<Any!, Any!>'
		 * This warning will become an error soon. See [KT-36770](https://youtrack.jetbrains.com/issue/KT-36770) for details
		 */
		override fun createProducer(txIdPrefix: String?): Producer<Any, Any> = MockProducer()
	}

	private val producerFactory: ProducerFactory<Any, Any> = KotlinProducerFactory()

	@Test
	fun `test instantiation with null txIdPrefix`() {
		val producer = producerFactory.createProducer(null)
		assertThat(producer).isInstanceOf(MockProducer::class.java)
	}

	@Test
	fun `test instantiation with non-null txIdPrefix`() {
		val producer = producerFactory.createProducer("foo")
		assertThat(producer).isInstanceOf(MockProducer::class.java)
	}
}
