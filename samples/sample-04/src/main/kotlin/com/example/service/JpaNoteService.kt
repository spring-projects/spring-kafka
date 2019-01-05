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
package com.example.service

import arrow.core.Option
import arrow.core.getOrElse
import arrow.data.NonEmptyList
import com.example.data.NoteRepository
import com.example.domain.Note
import com.example.domain.event.NoteEvent
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional

@Service
@Transactional
class JpaNoteService : NoteService {

    @Autowired
    private lateinit var noteRepository: NoteRepository
    @Autowired
    private lateinit var kafkaTemplate: KafkaTemplate<Any, Any>

    override fun getAllNotes(): Option<NonEmptyList<Note>> =
            NonEmptyList.fromList(noteRepository.findAll())

    override fun createNote(note: Option<Note>): Note {
        note.map {
            noteRepository.save(it)
            kafkaTemplate.send(TOPIC_NAME, NoteEvent.Created(it))
        }
        return note.getOrElse { Note(id = 0) }
    }

    @Override
    @Transactional(readOnly = true)
    override fun getNotesByAuthor(author: String): Option<NonEmptyList<Note>> {
        val noteList = noteRepository.findByAuthor(author)
        return NonEmptyList.fromList(noteList)
    }

    companion object {
        const val TOPIC_NAME = "notes"
    }
}
