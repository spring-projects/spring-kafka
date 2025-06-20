/*
 * Copyright 2022-present the original author or authors.
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

package org.springframework.kafka.config;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.BiPredicate;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import org.springframework.context.support.GenericApplicationContext;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.MessageListenerContainer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

/**
 * @author Gary Russell
 * @author Joo Hyuk Kim
 * @author Artem Bilan
 *
 * @since 2.8.9
 */
public class KafkaListenerEndpointRegistryTests {

	@Test
	void unregister() {
		KafkaListenerEndpointRegistry registry = new KafkaListenerEndpointRegistry();
		KafkaListenerEndpoint endpoint = mock(KafkaListenerEndpoint.class);
		@SuppressWarnings("unchecked")
		KafkaListenerContainerFactory<MessageListenerContainer> factory = mock(KafkaListenerContainerFactory.class);
		given(endpoint.getId()).willReturn("foo");
		MessageListenerContainer container = mock(MessageListenerContainer.class);
		given(factory.createListenerContainer(endpoint)).willReturn(container);
		registry.registerListenerContainer(endpoint, factory);
		MessageListenerContainer unregistered = registry.unregisterListenerContainer("foo");
		assertThat(unregistered).isSameAs(container);
		registry.registerListenerContainer(endpoint, factory);
		assertThat(unregistered).isSameAs(container);
	}

	@DisplayName("getListenerContainersMatching throws on null predicate")
	@Test
	void getListenerContainersMatchingThrowsOnNullPredicate() {
		// Given
		KafkaListenerEndpointRegistry registry = new KafkaListenerEndpointRegistry();
		// When & Then
		assertThatIllegalArgumentException()
			.isThrownBy(() -> registry.getListenerContainersMatching((Predicate<String>) null))
			.withMessage("'idMatcher' cannot be null");
	}

	@DisplayName("getListenerContainersMatching with BiPredicate throws on null biPredicate")
	@Test
	void getListenerContainersMatchingBiPredicateThrowsOnNullBiPredicate() {
		// Given
		KafkaListenerEndpointRegistry registry = new KafkaListenerEndpointRegistry();
		// When & Then
		assertThatIllegalArgumentException()
			.isThrownBy(() -> registry.getListenerContainersMatching((BiPredicate<String, MessageListenerContainer>) null))
			.withMessage("'idAndContainerMatcher' cannot be null");
	}

	@DisplayName("getListenerContainersMatching should return unmodifiable list")
	@Test
	void testGetListenerContainersMatchingReturnsUnmodifiableList() {
		// Given
		KafkaListenerEndpointRegistry registry = new KafkaListenerEndpointRegistry();
		registerListenerWithId(registry, "foo");
		// When
		Collection<MessageListenerContainer> listeners = registry.getListenerContainersMatching(s -> true);
		// Then
		assertThatExceptionOfType(UnsupportedOperationException.class)
			.isThrownBy(() -> listeners.add(mock(MessageListenerContainer.class)));
	}

	@ParameterizedTest(name = "getListenerContainersMatching({0}, {1}) = {2}")
	@MethodSource("paramsForGetListenerContainersMatching")
	void getListenerContainersMatching(List<String> names, Predicate<String> idMatcher, int expectedCount) {
		// Given
		KafkaListenerEndpointRegistry registry = new KafkaListenerEndpointRegistry();
		registerWithListenerIds(registry, names);
		// When
		Collection<MessageListenerContainer> listeners = registry.getListenerContainersMatching(idMatcher);
		// Then
		assertThat(listeners).hasSize(expectedCount);
	}

	/**
	 * Provides parameters for the getListenerContainersMatching test.
	 * Each set of parameters includes a list of names, a predicate, and the expected count of matching containers.
	 */
	private static Stream<Arguments> paramsForGetListenerContainersMatching() {
		List<String> names = List.of("foo", "bar", "baz");
		return Stream.of(
			// Case : Two names start with "b"
			Arguments.of(names, (Predicate<String>) id -> id.startsWith("b"), 2),
			// Case : One name starts with "f"
			Arguments.of(names, (Predicate<String>) id -> id.startsWith("f"), 1),
			// Case : Empty list
			Arguments.of(new ArrayList<>(), (Predicate<String>) id -> id.startsWith("b"), 0),
			// Case : All names match as the predicate always returns true
			Arguments.of(names, (Predicate<String>) id -> true, 3),
			// Case : No names match as the predicate always returns false
			Arguments.of(names, (Predicate<String>) id -> false, 0)
		);
	}

	@ParameterizedTest(name = "getListenerContainersMatching with BiPredicate for {0}, expecting {2} matches")
	@MethodSource("paramsForGetListenerContainersMatchingBiPredicate")
	void getListenerContainersMatchingBiPredicate(List<String> names, BiPredicate<String, MessageListenerContainer> idAndContainerMatcher, int expectedCount) {
		// Given
		KafkaListenerEndpointRegistry registry = new KafkaListenerEndpointRegistry();
		registerWithListenerIds(registry, names);
		// When
		Collection<MessageListenerContainer> listeners = registry.getListenerContainersMatching(idAndContainerMatcher);
		// Then
		assertThat(listeners).hasSize(expectedCount);
	}

	@Test
	void verifyUnregisteredListenerContainer() {
		KafkaListenerEndpointRegistry registry = new KafkaListenerEndpointRegistry();
		GenericApplicationContext applicationContext = new GenericApplicationContext();
		ConcurrentMessageListenerContainer<?, ?> listenerContainerMock = mock(ConcurrentMessageListenerContainer.class);
		given(listenerContainerMock.getListenerId()).willReturn("testListenerContainer");
		applicationContext.registerBean(ConcurrentMessageListenerContainer.class, () -> listenerContainerMock);
		applicationContext.refresh();
		registry.setApplicationContext(applicationContext);
		// Lazy-load from application context
		assertThat(registry.getUnregisteredListenerContainer("testListenerContainer")).isNotNull();
		// From internal map
		assertThat(registry.getUnregisteredListenerContainer("testListenerContainer")).isNotNull();
	}

	/**
	 * Provides parameters for the getListenerContainersMatchingBiPredicate test.
	 * Each set of parameters includes a list of names, a bi-predicate, and the expected count of matching containers.
	 */
	private static Stream<Arguments> paramsForGetListenerContainersMatchingBiPredicate() {
		List<String> names = List.of("foo", "bar", "baz");
		return Stream.of(
			// Case : Filters for names starting with "b" and containers that are "running"
			Arguments.of(names,
				(BiPredicate<String, MessageListenerContainer>) (id, container) -> id.startsWith("b") && container.isRunning(), 2),
			// Case : Filters for names starting with "f" and containers that are "running"
			Arguments.of(names,
				(BiPredicate<String, MessageListenerContainer>) (id, container) -> id.startsWith("f") && container.isRunning(), 1),
			// Case : Filters in an empty list of names
			Arguments.of(new ArrayList<>(),
				(BiPredicate<String, MessageListenerContainer>) (id, container) -> id.startsWith("b") && container.isRunning(), 0),
			// Case : Filters where all containers are considered "running"
			Arguments.of(names,
				(BiPredicate<String, MessageListenerContainer>) (id, container) -> container.isRunning(), 3),
			// Case : Filters where no containers are considered "running"
			Arguments.of(names,
				(BiPredicate<String, MessageListenerContainer>) (id, container) -> !container.isRunning(), 0)
		);
	}

	private static void registerWithListenerIds(KafkaListenerEndpointRegistry registry, List<String> names) {
		names.forEach(name -> registerListenerWithId(registry, name));
	}

	private static void registerListenerWithId(KafkaListenerEndpointRegistry registry, String id) {
		KafkaListenerEndpoint endpoint = mock(KafkaListenerEndpoint.class);
		@SuppressWarnings("unchecked")
		KafkaListenerContainerFactory<MessageListenerContainer> factory = mock(KafkaListenerContainerFactory.class);
		given(endpoint.getId()).willReturn(id);
		MessageListenerContainer container = mock(MessageListenerContainer.class);
		given(container.isRunning()).willReturn(true);
		given(factory.createListenerContainer(endpoint)).willReturn(container);
		registry.registerListenerContainer(endpoint, factory);
	}

}
