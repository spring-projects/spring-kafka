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
class JacksonJsonDeserializerTests {
    private val targetType = object : TypeReference<DeserializedData>() {}
    private val deserializer = JacksonJsonDeserializer(targetType).ignoreTypeHeaders()

    @Test
    fun `Expect deserializing a non-null value to work`() {
        val dataToDeserialize = """{"value":"test-data"}""".toByteArray(UTF_8)
        val deserializedData = deserializer.deserialize("topic", dataToDeserialize)

        assertThat(deserializedData).isEqualTo(DeserializedData("test-data"))
    }

    @Test
    fun `Expect deserializing a null value to work`() {
        val deserializedData = deserializer.deserialize("topic", null)
        assertThat(deserializedData).isNull()
    }

    private data class DeserializedData(val value: String)

}
