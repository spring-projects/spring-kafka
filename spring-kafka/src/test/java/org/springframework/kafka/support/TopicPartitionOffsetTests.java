/*
 * Copyright 2020-2022 the original author or authors.
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

import java.util.Objects;

import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import org.springframework.kafka.support.TopicPartitionOffset.SeekPosition;

/**
 * @author Gary Russell
 * @since 2.3.13
 *
 */
public class TopicPartitionOffsetTests {

	@Test
	void hashCodeTest() {
		assertThat(new TopicPartitionOffset("foo", 1, SeekPosition.BEGINNING).hashCode())
				.isNotEqualTo(new TopicPartitionOffset("foo",  1, SeekPosition.END).hashCode());
	}

	@Test
	void hashCodeNPE() {
		assertThat(new TopicPartitionOffset("foo", 0).hashCode())
				.isEqualTo(Objects.hash(new TopicPartition("foo", 0), null));
	}

}
