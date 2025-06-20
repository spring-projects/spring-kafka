/*
 * Copyright 2018-present the original author or authors.
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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.function.Predicate;

import org.springframework.util.Assert;

/**
 * Class for managing Allow / Deny collections and its predicates.
 *
 * @param <T> Collection generic type
 * @author Tomaz Fernandes
 * @since 28/12/20
 */
public final class AllowDenyCollectionManager<T>  {

	private final Collection<T> allowList;

	private final Collection<T> denyList;

	private final Collection<Predicate<T>> predicates;

	public AllowDenyCollectionManager(Collection<T> allowList, Collection<T> denyList) {
		this.allowList = allowList;
		this.denyList = denyList;
		this.predicates = Collections.singletonList(getDefaultPredicate(allowList, denyList));
	}

	public AllowDenyCollectionManager(Collection<T> allowList, Collection<T> denyList, Collection<Predicate<T>> predicates) {
		Assert.notNull(allowList, () -> "AllowList cannot be null");
		Assert.notNull(denyList, () -> "DenyList cannot be null");
		Assert.notNull(predicates, () -> "Predicates cannot be null");
		this.allowList = allowList;
		this.denyList = denyList;
		this.predicates = predicates;
	}

	public Predicate<T> getDefaultPredicate(Collection<T> allowList, Collection<T> denyList) {
		return objectToCheck -> !denyList.contains(objectToCheck)
				&& (allowList.isEmpty() || allowList.contains(objectToCheck));
	}

	public boolean isAllowed(T objectToCheck) {
		return this.predicates
				.stream()
				.allMatch(predicate -> predicate.test(objectToCheck));
	}

	public boolean areAllowed(T[] objects) {
		return Arrays.stream(objects)
				.allMatch(this::isAllowed);
	}

	public static <T> AllowDenyCollectionManager<T> createManagerFor(Collection<T> allowList, Collection<T> denyList) {
		return new AllowDenyCollectionManager<>(allowList, denyList);
	}

	public static <T> AllowDenyCollectionManager<T> createManagerFor(Collection<T> allowList, Collection<T> denyList, Collection<Predicate<T>> predicates) {
		return new AllowDenyCollectionManager<>(allowList, denyList, predicates);
	}

	public boolean hasNoRestrictions() {
		return this.allowList.isEmpty()
				&& this.denyList.isEmpty();
	}
}
