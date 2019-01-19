/*
 * Copyright 2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.kafka.support;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Predicate;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class KafkaStreamsBrancherTests {
	@Test
	@SuppressWarnings({"unchecked", "rawtypes"})
	void correctConsumersAreCalled() {
		Predicate p1 = Mockito.mock(Predicate.class);
		Predicate p2 = Mockito.mock(Predicate.class);
		KStream input = Mockito.mock(KStream.class);
		KStream[] result = new KStream[]{Mockito.mock(KStream.class),
				Mockito.mock(KStream.class), Mockito.mock(KStream.class)};
		Mockito.when(input.branch(Mockito.eq(p1), Mockito.eq(p2), Mockito.any()))
				.thenReturn(result);
		AtomicInteger invocations = new AtomicInteger(0);
		Assertions.assertSame(input, new KafkaStreamBrancher()
				.branch(
						p1,
						ks -> {
							Assertions.assertSame(result[0], ks);
							Assertions.assertEquals(0, invocations.getAndIncrement());
						})
				.defaultBranch(ks -> {
					Assertions.assertSame(result[2], ks);
					Assertions.assertEquals(2, invocations.getAndIncrement());
				})
				.branch(p2,
						ks -> {
							Assertions.assertSame(result[1], ks);
							Assertions.assertEquals(1, invocations.getAndIncrement());
						})
				.onTopOf(input));

		Assertions.assertEquals(3, invocations.get());
	}
}
