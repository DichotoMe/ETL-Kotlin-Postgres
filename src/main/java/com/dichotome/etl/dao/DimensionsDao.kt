package com.dichotome.etl.dao

import com.dichotome.etl.tables.dimensions.*

interface DimensionsDao {
    suspend fun createLocationDim(locationDim: LocationDim): Int

    suspend fun createObservationDateDim(observationDateDim: ObservationDateDim): Int

    suspend fun createReportingDateDim(reportingDateDim: ReportingDateDim): Int

    suspend fun createStatusDim(statusDim: StatusDim): Int

    suspend fun createDiseaseDim(diseaseDim: DiseaseDim): Int

    suspend fun createSerotypeDim(serotypeDim: SerotypeDim): Int

    suspend fun createSpeciesDim(speciesDim: SpeciesDim): Int

    suspend fun createSourceDim(sourceDim: SourceDim): Int
}