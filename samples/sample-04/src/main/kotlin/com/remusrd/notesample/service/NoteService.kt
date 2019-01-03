package com.remusrd.notesample.service

import com.remusrd.notesample.domain.Note
import arrow.core.Option
import arrow.data.NonEmptyList

interface NoteService {
    fun getNotesByAuthor(author: String) : Option<NonEmptyList<Note>>
    fun getAllNotes() : Option<NonEmptyList<Note>>
    fun createNote(note: Option<Note>): Note
}
