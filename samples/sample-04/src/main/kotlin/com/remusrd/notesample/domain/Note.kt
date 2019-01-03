package com.remusrd.notesample.domain

import java.time.LocalDateTime
import javax.persistence.*

@Entity
@Table(name = "note")
data class Note(
    @Id @GeneratedValue(strategy = GenerationType.IDENTITY)
    val id: Long,
    val content: String = "",
    val creationDate: LocalDateTime = LocalDateTime.now(),
    val lastModified: LocalDateTime = LocalDateTime.now(),
    val author: String = ""
)


