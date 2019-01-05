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

package com.example.domain

import java.time.LocalDateTime
import javax.persistence.*

@Entity
@Table(name = "note")
data class Note(
        @Id @GeneratedValue(strategy = GenerationType.IDENTITY)
        val id: Long,
        val content: String = "",
        val creationDate: LocalDateTime = LocalDateTime.now(),
        val lastModified: LocalDateTime = LocalDateTime.now(),
        val author: String = ""
)
