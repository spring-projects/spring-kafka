package com.example.data

import com.example.domain.Note
import org.springframework.data.jpa.repository.JpaRepository

interface NoteRepository: JpaRepository<Note,Long>{
    fun findByAuthor(author:String) : List<Note>
}
