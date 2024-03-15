/*
 * Copyright 2022-2024 the original author or authors.
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
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.kafka.listener.ListenerContainerRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;

/**
 * @author Gary Russell
 * @since 2.8.9
 *
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
		// When
		// Then
        assertThatIllegalArgumentException()
			.isThrownBy(() -> registry.getListenerContainersMatching(null))
            .withMessage("'idMatcher' cannot be null");
    }

	@Test
    void testGetListenerContainersMatchingReturnsUnmodifiableList() {
        // Given
        KafkaListenerEndpointRegistry registry = new KafkaListenerEndpointRegistry();
        registerListenerWithId(registry, "foo");
        // When
        Collection<MessageListenerContainer> listeners = registry.getListenerContainersMatching(s -> true);
        // Then
        assertThrows(UnsupportedOperationException.class, () -> listeners.add(mock(MessageListenerContainer.class)));
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
	 *
	 * @return Stream of Arguments for the parameterized test.
	 */
	private static Stream<Arguments> paramsForGetListenerContainersMatching() {
		List<String> names = List.of("foo", "bar", "baz");
		return Stream.of(
			// Test case: Two names start with "b"
			Arguments.of(names, (Predicate<String>) id -> id.startsWith("b"), 2),
			// Test case: One name starts with "f"
			Arguments.of(names, (Predicate<String>) id -> id.startsWith("f"), 1),
			// Corner case: Empty list
			Arguments.of(new ArrayList<>(), (Predicate<String>) id -> id.startsWith("b"), 0),
			// Corner case: All names match as the predicate always returns true
			Arguments.of(names, (Predicate<String>) id -> true, 3),
			// Corner case: No names match as the predicate always returns false
			Arguments.of(names, (Predicate<String>) id -> false, 0)
		);
	}

	private void registerWithListenerIds(KafkaListenerEndpointRegistry registry, List<String> names) {
		names.forEach(name -> registerListenerWithId(registry, name));
	}

	private void registerListenerWithId(KafkaListenerEndpointRegistry registry, String id) {
		KafkaListenerEndpoint endpoint = mock(KafkaListenerEndpoint.class);
		@SuppressWarnings("unchecked")
		KafkaListenerContainerFactory<MessageListenerContainer> factory = mock(KafkaListenerContainerFactory.class);
		given(endpoint.getId()).willReturn(id);
		MessageListenerContainer container = mock(MessageListenerContainer.class);
		given(factory.createListenerContainer(endpoint)).willReturn(container);
		registry.registerListenerContainer(endpoint, factory);
	}
}
