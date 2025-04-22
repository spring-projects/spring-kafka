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
public class PlaceVersion {

	@KafkaEntityKey
	private ComplexKeyRecord complexKeyRecord;

	private String placeName;

	public PlaceVersion() {
		super();
	}

	public PlaceVersion(ComplexKeyRecord complexKeyRecord, String placeName) {
		super();
		this.complexKeyRecord = complexKeyRecord;
		this.placeName = placeName;
	}

	public ComplexKeyRecord getComplexKeyRecord() {
		return complexKeyRecord;
	}

	public void setComplexKeyRecord(ComplexKeyRecord complexKeyRecord) {
		this.complexKeyRecord = complexKeyRecord;
	}

	public String getPlaceName() {
		return placeName;
	}

	public void setPlaceName(String placeName) {
		this.placeName = placeName;
	}

	@Override
	public int hashCode() {
		return Objects.hash(complexKeyRecord, placeName);
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
		PlaceVersion other = (PlaceVersion) obj;
		return Objects.equals(complexKeyRecord, other.complexKeyRecord)
				&& Objects.equals(placeName, other.placeName);
	}

	@Override
	public String toString() {
		return "PlaceVersion [complexKeyRecord=" + complexKeyRecord + ", placeName=" + placeName + "]";
	}

}
