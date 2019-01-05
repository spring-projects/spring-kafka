package com.example.notesample.data

import com.example.notesample.domain.Note
import org.springframework.data.jpa.repository.JpaRepository

interface NoteRepository: JpaRepository<Note,Long>{
    fun findByAuthor(author:String) : List<Note>
}
