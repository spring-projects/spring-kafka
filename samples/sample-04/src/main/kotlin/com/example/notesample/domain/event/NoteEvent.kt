package com.example.notesample.domain.event

import com.example.notesample.domain.Note
import com.example.notesample.domain.annotation.NoArg
import com.example.notesample.domain.annotation.Open

@NoArg
@Open
sealed class NoteEvent {
    abstract val note: Note

    data class Created(override val note: Note) : NoteEvent()
    data class Modified(override val note: Note) : NoteEvent()
    data class Deleted(override val note: Note) : NoteEvent()
}
