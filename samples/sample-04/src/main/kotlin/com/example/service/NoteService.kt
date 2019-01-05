package com.example.service

import arrow.core.Option
import arrow.data.NonEmptyList
import com.example.domain.Note

interface NoteService {
    fun getNotesByAuthor(author: String): Option<NonEmptyList<Note>>
    fun getAllNotes(): Option<NonEmptyList<Note>>
    fun createNote(note: Option<Note>): Note
}
