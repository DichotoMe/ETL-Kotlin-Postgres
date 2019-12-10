package com.dichotome.etl.tables

import com.dichotome.etl.tables.dimensions.*

data class Fact(
    val id: Int,
    val locationDim: LocationDim,
    val observationDateDim: ObservationDateDim,
    val reportingDateDim: ReportingDateDim,
    val statusDim: StatusDim,
    val diseaseDim: DiseaseDim,
    val serotypeDim: SerotypeDim,
    val speciesDim: SpeciesDim,
    val sourceDim: SourceDim,

    val animalsAtRisk: Int,
    val animalsAffected: Int,
    val animalsDeaths: Int,
    val animalsDestroyed: Int,
    val animalsSlaughtered: Int,
    val humansAffected: Int,
    val humansDeaths: Int
)