package com.dichotome.etl.db

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import java.sql.Connection
import java.sql.SQLException

object ConnectionPool {
    private val config = HikariConfig("./src/main/resources/db.properties")
    private val dataSource = HikariDataSource(config)

    val connection: Connection
        get() = try {
            dataSource.connection
        } catch (e: SQLException) {
            throw IllegalStateException("Can not get a connection")
        }
}