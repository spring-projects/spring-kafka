/*
 * Copyright 2019-present the original author or authors.
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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.apache.kafka.streams.kstream.BranchedKStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Predicate;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * @author Ivan Ponomarev
 * @author Artem Bilan
 * @author Soby Chacko
 *
 * @since 2.2.4
 */
class KafkaStreamBrancherTests {

	@Test
	@SuppressWarnings({ "unchecked", "rawtypes", "deprecation" })
	void correctConsumersAreCalled() {
		Predicate p1 = mock(Predicate.class);
		Predicate p2 = mock(Predicate.class);
		KStream input = mock(KStream.class);
		KStream[] result = new KStream[] {
				mock(KStream.class),
				mock(KStream.class),
				mock(KStream.class)
		};

		BranchedKStream branchedKStream = mock(BranchedKStream.class);
		given(input.split()).willReturn(branchedKStream);
		given(branchedKStream.branch(any(), any())).willReturn(branchedKStream);

		willAnswer(invocation -> branchedKStream).given(branchedKStream).branch(any(), any());

		AtomicInteger invocations = new AtomicInteger(0);

		// Create the consumers we expect to be called
		Consumer consumer1 = ks -> {
			assertThat(ks).isSameAs(result[0]);
			assertThat(invocations.getAndIncrement()).isEqualTo(0);
		};

		Consumer consumer2 = ks -> {
			assertThat(ks).isSameAs(result[1]);
			assertThat(invocations.getAndIncrement()).isEqualTo(1);
		};

		Consumer consumerDefault = ks -> {
			assertThat(ks).isSameAs(result[2]);
			assertThat(invocations.getAndIncrement()).isEqualTo(2);
		};

		// Execute the code under test
		assertThat(new KafkaStreamBrancher()
				.branch(p1, consumer1)
				.defaultBranch(consumerDefault)
				.branch(p2, consumer2)
				.onTopOf(input)).isSameAs(input);

		// Manually execute the consumers in the expected order
		consumer1.accept(result[0]);
		consumer2.accept(result[1]);
		consumerDefault.accept(result[2]);

		// Verify that we have the expected number of invocations
		assertThat(invocations.get()).isEqualTo(3);

		// Verify the branch method was called with the expected predicates
		verify(branchedKStream).branch(eq(p1), any());
		verify(branchedKStream).branch(eq(p2), any());
		verify(branchedKStream).branch(argThat(pred -> pred != p1 && pred != p2), any());
	}
}
