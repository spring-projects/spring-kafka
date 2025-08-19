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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.jspecify.annotations.Nullable;

import org.springframework.util.Assert;

/**
 * Configurable exception matcher with support for identifying matching exception in
 * nested causes. The matcher can be configured in two ways:
 * <ol>
 * <li>With an {@linkplain #forAllowList() allow list}, only the registered exception
 * matches. This includes exceptions that extend from any registered exception or
 * implement it if that's an interface.</li>
 * <li>With a {@linkplain #forDenyList() () deny list}, an exception match only if
 * it isn't found amongst the registered exceptions. As for the allowlist, any
 * exception that extends or implements one of the registered exceptions will lead it
 * to not match.</li>
 * </ol>
 * By default, only the given {@link Throwable} is inspected. To also search nested
 * causes, {@link #traverseCauses} should be enabled.
 *
 * @author Stephane Nicoll
 * @author Dave Syer
 * @author Gary Russell
 *
 * @since 4.0
 */
public class ExceptionMatcher {

	private final Map<Class<? extends Throwable>, Boolean> entries;

	private final boolean defaultMatch;

	private boolean traverseCauses;

	/**
	 * Create an instance with a list of exceptions. The {@code shouldMatchIfFound}
	 * parameter determines what should happen if an exception is found within that list.
	 * @param exceptionTypes the exceptions to register
	 * @param shouldMatchIfFound match result if a candidate exception is found amongst
	 * the given list
	 */
	public ExceptionMatcher(Collection<Class<? extends Throwable>> exceptionTypes, boolean shouldMatchIfFound) {
		this(buildEntries(exceptionTypes, shouldMatchIfFound), !shouldMatchIfFound, false);
	}

	protected ExceptionMatcher(Map<Class<? extends Throwable>, Boolean> entries,
			boolean matchIfNotFound, boolean traverseCauses) {

		this.entries = new HashMap<>(entries);
		this.defaultMatch = matchIfNotFound;
		this.traverseCauses = traverseCauses;
	}

	private static Map<Class<? extends Throwable>, Boolean> buildEntries(
			Collection<Class<? extends Throwable>> exceptionType, boolean value) {

		Map<Class<? extends Throwable>, Boolean> cache = new HashMap<>();
		for (Class<? extends Throwable> type : exceptionType) {
			cache.put(type, value);
		}
		return cache;
	}

	/**
	 * Create a matcher that matches any {@link Exception}, but not errors.
	 * @return a matcher that matches any exception, but not errors
	 */
	public static ExceptionMatcher defaultMatcher() {
		return new ExceptionMatcher(Collections.singletonMap(Exception.class, true), false, false);
	}

	/**
	 * Create a builder for a matcher that only matches an exception that is found in
	 * the configurable list of exception types.
	 * @return a {@link Builder} that configures an allowlist of exceptions
	 */
	public static Builder forAllowList() {
		return new Builder(true);
	}

	/**
	 * Create a builder for a matcher that only matches an exception that is not found in
	 * the configurable list of exception types.
	 * @return a {@link Builder} that configures a denylist of exceptions
	 */
	public static Builder forDenyList() {
		return new Builder(false);
	}

	/**
	 * Specify if this match should traverse nested causes to check for the
	 * presence of a matching exception.
	 * @param traverseCauses whether to traverse causes
	 */
	public void setTraverseCauses(boolean traverseCauses) {
		this.traverseCauses = traverseCauses;
	}

	/**
	 * Specify if the given {@link Throwable} match this instance.
	 * @param exception the exception to check
	 * @return {@code true} if this exception match this instance, {@code false} otherwise
	 */
	public boolean match(@Nullable Throwable exception) {
		boolean match = matchInCache(exception);
		if (!this.traverseCauses || exception == null) {
			return match;
		}
		/*
		 * If the result is the default, we need to find out if it was by default or so
		 * configured; if default, try the cause(es).
		 */
		if (match == this.defaultMatch) {
			Throwable cause = exception;
			do {
				if (this.entries.containsKey(cause.getClass())) {
					return match; // non-default classification
				}
				cause = cause.getCause();
				match = match(cause);
			}
			while (cause != null && match == this.defaultMatch);
		}
		return match;
	}

	protected Map<Class<? extends Throwable>, Boolean> getEntries() {
		return this.entries;
	}

	private boolean matchInCache(@Nullable Throwable classifiable) {
		if (classifiable == null) {
			return this.defaultMatch;
		}

		Class<? extends Throwable> exceptionClass = classifiable.getClass();
		if (this.entries.containsKey(exceptionClass)) {
			return this.entries.get(exceptionClass);
		}

		// check for subclasses
		Boolean value = null;
		for (Class<?> cls = exceptionClass.getSuperclass(); !cls.equals(Object.class)
				&& value == null; cls = cls.getSuperclass()) {
			value = this.entries.get(cls);
		}

		// check for interfaces subclasses
		if (value == null) {
			for (Class<?> cls = exceptionClass; !cls.equals(Object.class) && value == null; cls = cls.getSuperclass()) {
				for (Class<?> ifc : cls.getInterfaces()) {
					value = this.entries.get(ifc);
					if (value != null) {
						break;
					}
				}
			}
		}

		// ConcurrentHashMap doesn't allow nulls
		if (value != null) {
			this.entries.put(exceptionClass, value);
		}
		if (value == null) {
			value = this.defaultMatch;
		}
		return value;
	}

	/**
	 * Fluent API for configuring an {@link ExceptionMatcher}.
	 */
	public static class Builder {

		private final boolean matchIfFound;

		private final Set<Class<? extends Throwable>> exceptionClasses = new LinkedHashSet<>();

		private boolean traverseCauses = false;

		protected Builder(boolean matchIfFound) {
			this.matchIfFound = matchIfFound;
		}

		/**
		 * Add an exception type.
		 * @param exceptionType the exception type to add
		 * @return {@code this}
		 */
		public Builder add(Class<? extends Throwable> exceptionType) {
			Assert.notNull(exceptionType, "Exception class can not be null");
			this.exceptionClasses.add(exceptionType);
			return this;
		}

		/**
		 * Add all exception types from the given collection.
		 * @param exceptionTypes the exception types to add
		 * @return {@code this}
		 */
		public Builder addAll(Collection<Class<? extends Throwable>> exceptionTypes) {
			this.exceptionClasses.addAll(exceptionTypes);
			return this;
		}

		/**
		 * Specify if the matcher should traverse nested causes to check for the presence
		 * of a matching exception.
		 * @param traverseCauses whether to traverse causes
		 * @return {@code this}
		 */
		public Builder traverseCauses(boolean traverseCauses) {
			this.traverseCauses = traverseCauses;
			return this;
		}

		/**
		 * Build an {@link ExceptionMatcher}.
		 * @return a new exception matcher
		 */
		public ExceptionMatcher build() {
			return new ExceptionMatcher(buildEntries(new ArrayList<>(this.exceptionClasses), this.matchIfFound),
					!this.matchIfFound, this.traverseCauses);
		}

	}

}
