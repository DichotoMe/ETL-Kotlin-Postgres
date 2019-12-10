package com.dichotome.etl.dao

import com.dichotome.etl.tables.Fact

interface FactDao {
    fun createFact(fact: Fact)

    fun createAllFacts(facts: List<Fact>)
}