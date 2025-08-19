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

package org.springframework.kafka.support;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.StreamCorruptedException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link ExceptionMatcher}.
 *
 * @author Stephane Nicoll
 */
class ExceptionMatcherTests {

	@Test
	void matchWithDefaultMatcherAndException() {
		assertThat(ExceptionMatcher.defaultMatcher().match(new IllegalStateException())).isTrue();
	}

	@Test
	void matchWithDefaultMatcherAndError() {
		assertThat(ExceptionMatcher.defaultMatcher().match(new OutOfMemoryError())).isFalse();
	}

	@Test
	void matchWithDefaultMatcherAndNull() {
		assertThat(ExceptionMatcher.defaultMatcher().match(new IllegalStateException())).isTrue();
	}

	@Test
	void matchWithAllowList() {
		ExceptionMatcher classifier = ExceptionMatcher.forAllowList()
				.addAll(List.of(IOException.class, TimeoutException.class))
				.build();

		assertThat(classifier.match(new IOException())).isTrue();
		// Should not match nested causes
		assertThat(classifier.match(new RuntimeException(new IOException()))).isFalse();
		assertThat(classifier.match(new StreamCorruptedException())).isTrue();
		assertThat(classifier.match(new OutOfMemoryError())).isFalse();
	}

	@Test
	public void matchWithAllowListAndTraversingCauses() {
		ExceptionMatcher classifier = ExceptionMatcher.forAllowList()
				.add(IOException.class)
				.add(TimeoutException.class)
				.traverseCauses(true)
				.build();

		assertThat(classifier.match(new IOException())).isTrue();
		// Should match nested causes
		assertThat(classifier.match(new RuntimeException(new IOException()))).isTrue();
		assertThat(classifier.match(new StreamCorruptedException())).isTrue();
		// Should match due to FileNotFoundException being a subclass of TimeoutException
		assertThat(classifier.match(new FileNotFoundException())).isTrue();
		assertThat(classifier.match(new RuntimeException())).isFalse();
	}

	@Test
	public void matchWithDenyList() {
		ExceptionMatcher classifier = ExceptionMatcher.forDenyList()
				.addAll(List.of(Error.class, InterruptedException.class))
				.build();

		assertThat(classifier.match(new Throwable())).isTrue();
		assertThat(classifier.match(new InterruptedException())).isFalse();
		// should match due to OutOfMemoryError being a subclass of Error
		assertThat(classifier.match(new OutOfMemoryError())).isFalse();
		// Should match nested causes
		assertThat(classifier.match(new RuntimeException(new InterruptedException()))).isTrue();
	}

	@Test
	public void matchWithDenyListAndTraversingCauses() {
		ExceptionMatcher classifier = ExceptionMatcher.forDenyList()
				.add(Error.class)
				.add(InterruptedException.class)
				.traverseCauses(true)
				.build();

		assertThat(classifier.match(new Throwable())).isTrue();
		assertThat(classifier.match(new InterruptedException())).isFalse();
		// should retry due to traverseCauses=true
		assertThat(classifier.match(new RuntimeException(new InterruptedException()))).isFalse();
	}

	@Test
	void matchWithNullException() {
		ExceptionMatcher classifier = ExceptionMatcher.forAllowList()
				.add(Error.class).build();
		assertThat(classifier.match(null)).isFalse();
	}

	@Test
	public void creteWithEmptyAllowList() {
		ExceptionMatcher classifier = new ExceptionMatcher(Collections.emptyList(), true);
		assertThat(classifier.match(new IllegalStateException("foo"))).isFalse();
	}

	@Test
	public void creteWithEmptyDenyList() {
		ExceptionMatcher classifier = new ExceptionMatcher(Collections.emptyList(), false);
		assertThat(classifier.match(new IllegalStateException("foo"))).isTrue();
	}

	@Test
	public void createWithAllowListAndExactMatch() {
		Collection<Class<? extends Throwable>> set = Collections.singleton(IllegalStateException.class);
		assertThat(new ExceptionMatcher(set, true).match(new IllegalStateException("Foo"))).isTrue();
	}

	@Test
	public void createWithAllowListAndMatchInCause() {
		Collection<Class<? extends Throwable>> set = Collections.singleton(IllegalStateException.class);
		ExceptionMatcher exceptionMatcher = new ExceptionMatcher(set, true);
		exceptionMatcher.setTraverseCauses(true);
		assertThat(exceptionMatcher.match(new RuntimeException(new IllegalStateException("Foo")))).isTrue();
	}

	@Test
	public void createWithAllowListAndMatchInSubClassCause() {
		Collection<Class<? extends Throwable>> set = Collections.singleton(IllegalStateException.class);
		ExceptionMatcher exceptionMatcher = new ExceptionMatcher(set, true);
		exceptionMatcher.setTraverseCauses(true);
		assertThat(exceptionMatcher.match(new RuntimeException(new FooException("Foo")))).isTrue();
	}

	@Test
	public void createWithNoMatchInSubClassUpdatesCache() {
		ExceptionMatcher exceptionMatcher = new ExceptionMatcher(
				Map.of(IllegalStateException.class, true, BarException.class, false), true, true);
		assertThat(exceptionMatcher.match(new RuntimeException(new FooException("Foo", new BarException())))).isTrue();
		assertThat(exceptionMatcher).extracting("entries")
				.asInstanceOf(InstanceOfAssertFactories.map(Class.class, Boolean.class))
				.containsEntry(FooException.class, true);
	}

	@SuppressWarnings("serial")
	static class FooException extends IllegalStateException {

		FooException(String s) {
			super(s);
		}

		FooException(String s, Throwable t) {
			super(s, t);
		}

	}

	@SuppressWarnings("serial")
	static class BarException extends RuntimeException {

		BarException() {
			super();
		}

	}

}
