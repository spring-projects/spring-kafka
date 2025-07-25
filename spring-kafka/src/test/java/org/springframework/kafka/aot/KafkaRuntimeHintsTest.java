/*
 * Copyright 2024-present the original author or authors.
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

package org.springframework.kafka.aot;

import org.junit.jupiter.api.Test;

import org.springframework.aot.hint.MemberCategory;
import org.springframework.aot.hint.RuntimeHints;
import org.springframework.aot.hint.predicate.RuntimeHintsPredicates;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonTypeResolver;
import org.springframework.kafka.support.mapping.DefaultJackson2JavaTypeMapper;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link KafkaRuntimeHints} specifically focusing on JsonDeserializer
 * dynamic class loading and GraalVM native image compatibility.
 *
 * @author Claude (AI Assistant)
 * @since 3.4
 */
class KafkaRuntimeHintsTest {

	private final KafkaRuntimeHints kafkaHints = new KafkaRuntimeHints();

	@Test
	void shouldRegisterJsonDeserializerHints() {
		RuntimeHints hints = new RuntimeHints();
		this.kafkaHints.registerHints(hints, getClass().getClassLoader());

		// Verify JsonDeserializer basic hints
		assertThat(RuntimeHintsPredicates.reflection()
				.onType(JsonDeserializer.class)
				.withMemberCategory(MemberCategory.INVOKE_PUBLIC_CONSTRUCTORS))
				.accepts(hints);
	}

	@Test
	void shouldRegisterJsonTypeResolverHints() {
		RuntimeHints hints = new RuntimeHints();
		this.kafkaHints.registerHints(hints, getClass().getClassLoader());

		// Verify JsonTypeResolver reflection hints for dynamic class loading
		assertThat(RuntimeHintsPredicates.reflection()
				.onType(JsonTypeResolver.class)
				.withMemberCategory(MemberCategory.INVOKE_DECLARED_METHODS))
				.accepts(hints);
	}

	@Test
	void shouldRegisterDefaultJackson2JavaTypeMapperHints() {
		RuntimeHints hints = new RuntimeHints();
		this.kafkaHints.registerHints(hints, getClass().getClassLoader());

		// Verify type mapper hints for dynamic type resolution
		assertThat(RuntimeHintsPredicates.reflection()
				.onType(DefaultJackson2JavaTypeMapper.class)
				.withMemberCategory(MemberCategory.INVOKE_DECLARED_CONSTRUCTORS))
				.accepts(hints);
	}

	@Test
	void shouldRegisterJavaLangMethodHints() {
		RuntimeHints hints = new RuntimeHints();
		this.kafkaHints.registerHints(hints, getClass().getClassLoader());

		// Verify Method class hints for buildTypeResolver reflection
		assertThat(RuntimeHintsPredicates.reflection()
				.onType(java.lang.reflect.Method.class)
				.withMemberCategory(MemberCategory.INVOKE_PUBLIC_METHODS))
				.accepts(hints);
	}

	@Test
	void shouldRegisterClassForNameHints() {
		RuntimeHints hints = new RuntimeHints();
		this.kafkaHints.registerHints(hints, getClass().getClassLoader());

		// Verify Class hints for ClassUtils.forName() support
		assertThat(RuntimeHintsPredicates.reflection()
				.onType(Class.class)
				.withMemberCategory(MemberCategory.INVOKE_PUBLIC_METHODS))
				.accepts(hints);
	}

}
