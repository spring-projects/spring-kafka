package com.remusrd.notesample.domain.event

import com.remusrd.notesample.domain.Note
import com.remusrd.notesample.domain.annotation.NoArg
import com.remusrd.notesample.domain.annotation.Open

@NoArg
@Open
sealed class NoteEvent {
    abstract val note: Note

    data class Created(override val note: Note) : NoteEvent()
    data class Modified(override val note: Note) : NoteEvent()
    data class Deleted(override val note: Note) : NoteEvent()
}
