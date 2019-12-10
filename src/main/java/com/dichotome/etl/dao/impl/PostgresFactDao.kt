package com.dichotome.etl.dao.impl

import com.dichotome.etl.dao.FactDao
import com.dichotome.etl.db.ConnectionPool
import com.dichotome.etl.tables.Fact
import java.sql.PreparedStatement
import java.sql.SQLException

class PostgresFactDao : FactDao {
    companion object {
        const val INSERT_FACT = "INSERT INTO disease_facts " +
            "(" +
            "id," +
            "locationId," +
            "observationDateId," +
            "reportingDateId," +
            "statusId," +
            "diseaseId," +
            "serotypeId," +
            "speciesId," +
            "sourceId," +

            "animalsAtRisk," +
            "animalsAffected," +
            "animalsDeaths," +
            "animalsDestroyed," +
            "animalsSlaughtered," +
            "humansAffected," +
            "humansDeaths" +
            ") values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    }

    private fun populateInsertionQuery(query: PreparedStatement, fact: Fact) {
        with(query) {
            setInt(1, fact.id)
            setInt(2, fact.locationDim.id!!)
            setInt(3, fact.observationDateDim.id!!)
            setInt(4, fact.reportingDateDim.id!!)
            setInt(5, fact.statusDim.id!!)
            setInt(6, fact.diseaseDim.id!!)
            setInt(7, fact.serotypeDim.id!!)
            setInt(8, fact.speciesDim.id!!)
            setInt(9, fact.sourceDim.id!!)

            setInt(10, fact.animalsAtRisk)
            setInt(11, fact.animalsAffected)
            setInt(12, fact.animalsDeaths)
            setInt(13, fact.animalsDestroyed)
            setInt(14, fact.animalsSlaughtered)
            setInt(15, fact.humansAffected)
            setInt(16, fact.humansDeaths)
        }
    }

    override fun createFact(fact: Fact) {
        try {
            ConnectionPool.connection.use { conn ->
                conn.prepareStatement(INSERT_FACT).use { statement ->
                    populateInsertionQuery(statement, fact)
                    statement.executeUpdate()
                }
            }
        } catch (e: SQLException) {
            e.printStackTrace()
        }
    }

    override fun createAllFacts(facts: List<Fact>) {
        try {
            ConnectionPool.connection.use { conn ->
                conn.prepareStatement(INSERT_FACT).use { statement ->
                    facts.forEach {
                        try {
                            populateInsertionQuery(statement, it)
                            statement.executeUpdate()
                        } catch (e: SQLException) {
                            if (!isUniqueConstraintException(e)) throw e
                        }
                    }
                }
            }
        } catch (e: SQLException) {
            e.printStackTrace()
        }
    }

    private fun isUniqueConstraintException(e: SQLException) = e.sqlState == "23505"
}