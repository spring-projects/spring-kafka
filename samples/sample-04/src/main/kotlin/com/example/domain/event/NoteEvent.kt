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
