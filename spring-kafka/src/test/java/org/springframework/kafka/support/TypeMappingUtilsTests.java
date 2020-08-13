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

package org.springframework.kafka.support;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

import org.junit.jupiter.api.Test;

import org.springframework.context.support.GenericApplicationContext;
import org.springframework.kafka.annotation.TypeMapping;

/**
 * TODO: Move to spring-messaging?
 *
 * @author Gary Russell
 * @since 2.6
 *
 */
public class TypeMappingUtilsTests {

	@Test
	void testDiscover() {
		Map<String, Class<?>> mappings =
				TypeMappingUtils.findTypeMappings(new GenericApplicationContext(), getClass().getPackage().getName());
		assertThat(mappings).hasSize(6); // this will be 3 if we move this to s-m
		assertThat(mappings.get("fooA")).isEqualTo(Foo.class);
		assertThat(mappings.get("fooB")).isEqualTo(Foo.class);
		assertThat(mappings.get("barA")).isEqualTo(Bar.class);
	}

	@TypeMapping({ "fooA", "fooB" })
	public static class Foo {

		public String foo = "foo";

	}

	@TypeMapping("barA")
	public static class Bar extends Foo {

		public String bar = "bar";

	}

}
