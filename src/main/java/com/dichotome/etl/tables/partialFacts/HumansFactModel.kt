package com.dichotome.etl.tables.partialFacts

data class HumansFactModel(
    val id: String,
    val humansAffected: String,
    val humansDeaths: String
) : PartFact