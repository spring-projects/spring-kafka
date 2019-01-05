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

package com.example.controller

import arrow.core.Option
import com.example.domain.Note
import com.example.service.NoteService
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.web.bind.annotation.*

@RestController
@RequestMapping("/notes")
class NoteController {
    @Autowired
    private lateinit var noteService: NoteService

    @GetMapping
    fun getAllNotes(): Option<List<Note>> {
        return noteService.getAllNotes().flatMap { Option.just(it.all) }
    }

    @PostMapping
    fun postNote(@RequestBody note: Note): Note {
        return noteService.createNote(Option(note))

    }

}
