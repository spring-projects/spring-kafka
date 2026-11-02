/*
 * Copyright 2026-present the original author or authors.
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
