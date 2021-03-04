/*
 * Copyright 2019-2021 the original author or authors.
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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Predicate;
import org.junit.jupiter.api.Test;

/**
 * @author Ivan Ponomarev
 * @author Artem Bilan
 *
 * @since 2.2.4
 */
class KafkaStreamBrancherTests {

	@Test
	@SuppressWarnings({ "unchecked", "rawtypes" })
	void correctConsumersAreCalled() {
		Predicate p1 = mock(Predicate.class);
		Predicate p2 = mock(Predicate.class);
		KStream input = mock(KStream.class);
		KStream[] result =
				new KStream[] { mock(KStream.class), mock(KStream.class), mock(KStream.class) };
		given(input.branch(eq(p1), eq(p2), any()))
				.willReturn(result);
		AtomicInteger invocations = new AtomicInteger(0);
		assertThat(new KafkaStreamBrancher()
				.branch(
						p1,
						ks -> {
							assertThat(ks).isSameAs(result[0]);
							assertThat(invocations.getAndIncrement()).isEqualTo(0);
						})
				.defaultBranch(ks -> {
					assertThat(ks).isSameAs(result[2]);
					assertThat(invocations.getAndIncrement()).isEqualTo(2);
				})
				.branch(p2,
						ks -> {
							assertThat(ks).isSameAs(result[1]);
							assertThat(invocations.getAndIncrement()).isEqualTo(1);
						})
				.onTopOf(input)).isSameAs(input);

		assertThat(invocations.get()).isEqualTo(3);
	}

}
