package com.dichotome.etl.tables.partialFacts

data class AnimalsFactModel(
    val id: String,
    val animalsAtRisk: String,
    val animalsAffected: String,
    val animalsDeaths: String,
    val animalsDestroyed: String,
    val animalsSlaughtered: String
) : PartFact