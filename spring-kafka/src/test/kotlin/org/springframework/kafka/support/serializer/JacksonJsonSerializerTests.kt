/*
 * Copyright 2016-present the original author or authors.
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

package org.springframework.kafka.support.serializer

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import tools.jackson.core.type.TypeReference
import kotlin.text.Charsets.UTF_8

/**
 * @author Trond Ziarkowski
 *
 * @since 4.0.1
 */
class JacksonJsonSerializerTests {
    private val targetType = object : TypeReference<DataToSerialize>() {}
    private val serializer = JacksonJsonSerializer(targetType).noTypeInfo()

    @Test
    fun `Expect serializing a non-null value to work`() {
        val dataToSerialize = DataToSerialize("test-data")
        val bytes = serializer.serialize("topic", dataToSerialize)

        assertThat(bytes?.toString(UTF_8)).isEqualTo("""{"value":"test-data"}""")
    }

    @Test
    fun `Expect serializing a null value to work`() {
        val bytes = serializer.serialize("topic", null)
        assertThat(bytes).isNull()
    }

    private data class DataToSerialize(val value: String)
}
