/*
 * Copyright 2020 the original author or authors.
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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import org.springframework.util.Assert;

/**
 * A {@link Supplier} for bootstrap servers that can toggle between 2 lists of servers.
 *
 * @author Gary Russell
 * @since 2.5
 *
 */
public class ABSwitchCluster implements Supplier<String> {

	private final AtomicBoolean which = new AtomicBoolean(true);

	private final String primary;

	private final String secondary;

	/**
	 * Construct an instance with primary and secondary bootstrap servers.
	 * @param primary the primary.
	 * @param secondary the secondary.
	 */
	public ABSwitchCluster(String primary, String secondary) {
		Assert.hasText(primary, "'primary' is required");
		Assert.hasText(secondary, "'secondary' is required");
		this.primary = primary;
		this.secondary = secondary;
	}

	@Override
	public String get() {
		return this.which.get() ? this.primary : this.secondary;
	}

	/**
	 * Get whether or not the primary cluster is active.
	 * @return true for primary, false for secondary.
	 */
	public boolean isPrimary() {
		return this.which.get();
	}

	/**
	 * Use the primary cluster.
	 */
	public void primary() {
		this.which.set(true);
	}

	/**
	 * Use the secondary cluster.
	 */
	public void secondary() {
		this.which.set(false);
	}

}
