/*
 * Copyright 2016-2025 the original author or authors.
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

package org.springframework.kafka.entity;

import java.util.Objects;

/**
 * @author Popovics Boglarka
 */
@KafkaEntity
public class CityGroup {
	@KafkaEntityKey
	private ComplexKey complexKey;

	private String country;

	public CityGroup() {
		super();
	}

	public CityGroup(ComplexKey complexKey, String country) {
		super();
		this.complexKey = complexKey;
		this.country = country;
	}

	public ComplexKey getComplexKey() {
		return complexKey;
	}

	public void setComplexKey(ComplexKey complexKey) {
		this.complexKey = complexKey;
	}

	public String getCountry() {
		return country;
	}

	public void setCountry(String country) {
		this.country = country;
	}

	@Override
	public int hashCode() {
		return Objects.hash(complexKey, country);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		CityGroup other = (CityGroup) obj;
		return Objects.equals(complexKey, other.complexKey) && Objects.equals(country, other.country);
	}

	@Override
	public String toString() {
		return "CityGroup [complexKey=" + complexKey + ", country=" + country + "]";
	}

}
