package com.remusrd.notesample.data

import com.remusrd.notesample.domain.Note
import org.springframework.data.jpa.repository.JpaRepository

interface NoteRepository: JpaRepository<Note,Long>{
    fun findByAuthor(author:String) : List<Note>
}
