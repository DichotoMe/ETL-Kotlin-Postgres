package com.dichotome.etl.tables.partialFacts

data class DiseaseInfoFactModel (
    val name: String,
    val type: String,
    val target: String,
    val infectsHumans: String
)