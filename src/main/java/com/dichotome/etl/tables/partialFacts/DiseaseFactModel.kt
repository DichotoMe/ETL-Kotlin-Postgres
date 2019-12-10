package com.dichotome.etl.tables.partialFacts


data class DiseaseFactModel(
    val id: String,
    val latitude: String,
    val longitude: String,
    val region: String,
    val country: String,
    val localityname: String,
    val observationDate: String,
    val reportingDate: String,
    val status: String,
    val disease: String,
    val serotype: String,
    val species: String,
    val source: String
) : PartFact