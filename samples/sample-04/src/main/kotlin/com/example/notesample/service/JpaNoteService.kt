package com.example.notesample.service

import com.example.notesample.data.NoteRepository
import com.example.notesample.domain.Note
import com.example.notesample.domain.event.NoteEvent
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import arrow.core.Option
import arrow.core.getOrElse
import arrow.data.NonEmptyList

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
