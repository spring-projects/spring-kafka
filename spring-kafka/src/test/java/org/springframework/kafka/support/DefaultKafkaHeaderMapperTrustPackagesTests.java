/*
 * Copyright 2026-present the original author or authors.
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

package org.springframework.kafka.support;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for trusting packages in {@link DefaultKafkaHeaderMapper}.
 *
 * @author Soby Chacko
 */
public class DefaultKafkaHeaderMapperTrustPackagesTests {

	@Test
	void subpackagesOfDefaultsAreNotTrustedTransitively() {
		DefaultKafkaHeaderMapper mapper = new DefaultKafkaHeaderMapper();
		assertThat(mapper.trusted("java.util.logging.FileHandler")).isFalse();
		assertThat(mapper.trusted("java.lang.reflect.Method")).isFalse();
		assertThat(mapper.trusted("java.util.concurrent.ForkJoinPool")).isFalse();
	}

	@Test
	void exactMatchAgainstDefaultsStillWorks() {
		DefaultKafkaHeaderMapper mapper = new DefaultKafkaHeaderMapper();
		assertThat(mapper.trusted("java.util.HashMapr")).isTrue();
		assertThat(mapper.trusted("java.lang.String")).isTrue();
		assertThat(mapper.trusted("java.util.UUID")).isTrue();
	}

	@Test
	void userAddedSubpackagesMustBeAddedExplicitly() {
		DefaultKafkaHeaderMapper mapper = new DefaultKafkaHeaderMapper();
		mapper.addTrustedPackages("com.acme");
		assertThat(mapper.trusted("com.acme.OrderEvent")).isTrue();
		assertThat(mapper.trusted("com.acme.events.OrderEvent")).isFalse();
		mapper.addTrustedPackages("com.acme.events");
		assertThat(mapper.trusted("com.acme.events.OrderEvent")).isTrue();
	}

}
