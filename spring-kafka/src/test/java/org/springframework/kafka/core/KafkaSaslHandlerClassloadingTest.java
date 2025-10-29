/*
 * Copyright 2025-present the original author or authors.
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

package org.springframework.kafka.core;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import org.springframework.context.support.GenericApplicationContext;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for GH-4109: Verifies that DefaultKafkaProducerFactory switches the thread context
 * classloader to the application context's classloader during producer creation.
 * <p>
 * This fixes an issue where SASL callback handler classes (and other Kafka-loaded classes)
 * specified by name in configuration cannot be found when accessed from background threads
 * in Spring Boot fat JARs, because the thread's context classloader cannot see classes in
 * BOOT-INF/lib. By switching to the application context's classloader, all nested JAR
 * classes become accessible.
 *
 * @author Alexandros Papadakis
 * @author Soby Chacko
 */
class KafkaSaslHandlerClassloadingTest {

	@Test
	void switchesToApplicationContextClassLoaderDuringProducerCreation() {
		ClassLoader customLoader = new CustomClassLoader();
		ClassLoader originalTCCL = Thread.currentThread().getContextClassLoader();

		Map<String, Object> configs = producerConfigs();

		try (GenericApplicationContext ctx = new GenericApplicationContext()) {
			ctx.setClassLoader(customLoader);

			// Use a test factory that captures the TCCL during producer creation
			AtomicReference<ClassLoader> capturedLoader = new AtomicReference<>();
			TestableProducerFactory<String, String> factory = new TestableProducerFactory<>(configs, capturedLoader);

			ctx.registerBean("factory", DefaultKafkaProducerFactory.class, () -> factory);
			ctx.refresh();

			// Trigger producer creation
			try {
				factory.createProducer();
			}
			catch (Exception e) {
				// Expected to fail (no broker), but we've captured the classloader
			}

			// Verify that during producer creation, TCCL was switched to app context classloader
			assertThat(capturedLoader.get())
					.as("TCCL should be switched to application context classloader during producer creation")
					.isSameAs(customLoader);

			// Verify TCCL was restored after
			assertThat(Thread.currentThread().getContextClassLoader())
					.as("TCCL should be restored to original after producer creation")
					.isSameAs(originalTCCL);
		}
	}

	@Test
	void fallsBackToFactoryClassLoaderWhenNoApplicationContext() {
		ClassLoader originalTCCL = Thread.currentThread().getContextClassLoader();
		Map<String, Object> configs = producerConfigs();

		AtomicReference<ClassLoader> capturedLoader = new AtomicReference<>();
		TestableProducerFactory<String, String> factory = new TestableProducerFactory<>(configs, capturedLoader);

		// No application context - factory not managed by Spring
		try {
			factory.createProducer();
		}
		catch (Exception e) {
			// Expected to fail (no broker), but we've captured the classloader
		}

		// Verify that TCCL was switched to factory's classloader as fallback
		assertThat(capturedLoader.get())
				.as("TCCL should be switched to factory classloader when no app context")
				.isSameAs(factory.getClass().getClassLoader());

		// Verify TCCL was restored
		assertThat(Thread.currentThread().getContextClassLoader())
				.as("TCCL should be restored to original")
				.isSameAs(originalTCCL);
	}

	private Map<String, Object> producerConfigs() {
		Map<String, Object> configs = new HashMap<>();
		configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		return configs;
	}

	/**
	 * Test factory that captures the thread context classloader at the point
	 * where KafkaProducer would be instantiated (after our classloader switching).
	 */
	static class TestableProducerFactory<K, V> extends DefaultKafkaProducerFactory<K, V> {

		private final AtomicReference<ClassLoader> capturedLoader;

		TestableProducerFactory(Map<String, Object> configs, AtomicReference<ClassLoader> capturedLoader) {
			super(configs);
			this.capturedLoader = capturedLoader;

			// Override the key serializer supplier to capture classloader and abort
			this.setKeySerializerSupplier(() -> {
				// At this point, we're inside the parent's createRawProducer try block,
				// AFTER the classloader has been switched
				this.capturedLoader.set(Thread.currentThread().getContextClassLoader());
				throw new TestAbortedException();
			});
		}
	}

	/**
	 * Marker exception to exit producer creation after capturing classloader.
	 */
	static class TestAbortedException extends RuntimeException {
	}

	/**
	 * Simple custom classloader for testing.
	 */
	static class CustomClassLoader extends ClassLoader {
		CustomClassLoader() {
			super(KafkaSaslHandlerClassloadingTest.class.getClassLoader());
		}
	}
}
