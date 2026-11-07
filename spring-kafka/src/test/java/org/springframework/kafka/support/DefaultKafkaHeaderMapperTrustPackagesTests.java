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
	void deeplyNestedArrayTypeIsNotTrusted() {
		DefaultKafkaHeaderMapper mapper = new DefaultKafkaHeaderMapper();
		assertThat(mapper.trusted("java.lang.String" + "[]".repeat(100_000))).isFalse();
		assertThat(mapper.trusted("[".repeat(100_000) + "Ljava.lang.String;")).isFalse();
		assertThat(mapper.trusted("java.lang.String" + "[]".repeat(255))).isTrue();
		assertThat(mapper.trusted("java.lang.String" + "[]".repeat(256))).isFalse();
	}

	@Test
	void sourceFormArrayTypesOfUntrustedTypesAreNeverTrusted() {
		DefaultKafkaHeaderMapper mapper = new DefaultKafkaHeaderMapper();
		mapper.addTrustedPackages("*");
		assertThat(mapper.trusted("java.net.InetAddress[]")).isFalse();
		assertThat(mapper.trusted("java.net.Inet4Address[][]")).isFalse();
		assertThat(mapper.trusted("java.lang.Class[]")).isFalse();
		assertThat(mapper.trusted("org.springframework.util.ConcurrentReferenceHashMap[]")).isFalse();
		assertThat(mapper.trusted("java.lang.String[][]")).isTrue();
	}

	@Test
	void threadCreatingTypesAreNeverTrusted() {
		DefaultKafkaHeaderMapper mapper = new DefaultKafkaHeaderMapper();
		assertThat(mapper.trusted("java.util.Timer")).isFalse();
		assertThat(mapper.trusted("[Ljava.util.Timer;")).isFalse();
		mapper.addTrustedPackages("*");
		assertThat(mapper.trusted("java.util.Timer")).isFalse();
		assertThat(mapper.trusted("[Ljava.util.Timer;")).isFalse();
		assertThat(mapper.trusted("java.util.Timer[][]")).isFalse();
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

	@Test
	void classTypeIsNeverTrusted() {
		DefaultKafkaHeaderMapper mapper = new DefaultKafkaHeaderMapper();
		assertThat(mapper.trusted("java.lang.Class")).isFalse();
		assertThat(mapper.trusted("[Ljava.lang.Class;")).isFalse();
		assertThat(mapper.trusted("[[Ljava.lang.Class;")).isFalse();
		mapper.addTrustedPackages("*");
		assertThat(mapper.trusted("java.lang.Class")).isFalse();
		assertThat(mapper.trusted("[Ljava.lang.Class;")).isFalse();
	}

	@Test
	void memoryAllocatingTypesAreNeverTrusted() {
		DefaultKafkaHeaderMapper mapper = new DefaultKafkaHeaderMapper();
		assertThat(mapper.trusted("org.springframework.util.ResizableByteArrayOutputStream")).isFalse();
		assertThat(mapper.trusted("org.springframework.util.ConcurrentReferenceHashMap")).isFalse();
		assertThat(mapper.trusted("[Lorg.springframework.util.ResizableByteArrayOutputStream;")).isFalse();
		assertThat(mapper.trusted("org.springframework.util.LinkedMultiValueMap")).isTrue();
		mapper.addTrustedPackages("*");
		assertThat(mapper.trusted("org.springframework.util.ResizableByteArrayOutputStream")).isFalse();
		assertThat(mapper.trusted("org.springframework.util.ConcurrentReferenceHashMap")).isFalse();
	}

}
