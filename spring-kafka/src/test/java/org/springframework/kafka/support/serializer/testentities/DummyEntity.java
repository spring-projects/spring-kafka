/*
 * Copyright 2016-2019 the original author or authors.
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

package org.springframework.kafka.support.serializer.testentities;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * @author Igor Stepanov
 */
public class DummyEntity {

	public int intValue;

	public Long longValue;

	public String stringValue;

	public Map<Short, List<String>> complexStruct;

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		DummyEntity that = (DummyEntity) o;
		return intValue == that.intValue &&
				Objects.equals(longValue, that.longValue) &&
				Objects.equals(stringValue, that.stringValue) &&
				Objects.equals(complexStruct, that.complexStruct);
	}

	@Override
	public int hashCode() {
		return Objects.hash(intValue, longValue, stringValue, complexStruct);
	}

}
