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
package com.example.domain.event

import com.example.domain.Note
import com.example.domain.annotation.NoArg
import com.example.domain.annotation.Open

@NoArg
@Open
sealed class NoteEvent {
    abstract val note: Note

    data class Created(override val note: Note) : NoteEvent()
    data class Modified(override val note: Note) : NoteEvent()
    data class Deleted(override val note: Note) : NoteEvent()
}
