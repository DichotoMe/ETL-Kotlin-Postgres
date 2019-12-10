package com.dichotome.etl.utils

import com.dichotome.etl.tables.Fact
import com.dichotome.etl.tables.dimensions.*
import java.util.concurrent.ConcurrentHashMap

object DimensionsStage {
    private val locationDimMap = ConcurrentHashMap<Pair<Double, Double>, LocationDim>()
    private val observationDateDimMap = ConcurrentHashMap<String, ObservationDateDim>()
    private val reportingDateDimMap = ConcurrentHashMap<String, ReportingDateDim>()
    private val statusDimMap = ConcurrentHashMap<String, StatusDim>()
    private val diseaseDimMap = ConcurrentHashMap<String, DiseaseDim>()
    private val serotypeDimMap = ConcurrentHashMap<String, SerotypeDim>()
    private val speciesDimMap = ConcurrentHashMap<String, SpeciesDim>()
    private val sourceDimMap = ConcurrentHashMap<String, SourceDim>()

    val facts = ArrayList<Fact>()

    val locationDims = locationDimMap.values
    val observationDateDims = observationDateDimMap.values
    val reportingDateDims = reportingDateDimMap.values
    val statusDims = statusDimMap.values
    val diseaseDims = diseaseDimMap.values
    val serotypeDims = serotypeDimMap.values
    val speciesDims = speciesDimMap.values
    val sourceDims = sourceDimMap.values

    fun getLocationDim(
        latitude: Double,
        longitude: Double,
        region: String,
        country: String,
        localityName: String
    ): LocationDim =
        locationDimMap[latitude to longitude] ?: LocationDim(
            null,
            latitude,
            longitude,
            region,
            country,
            localityName
        ).also {
            locationDimMap[latitude to longitude] = it
        }

    fun getObservationDateDim(date: String): ObservationDateDim =
        observationDateDimMap[date] ?: ObservationDateDim(null, date).also {
            observationDateDimMap[date] = it
        }

    fun getReportingDateDim(date: String): ReportingDateDim =
        reportingDateDimMap[date] ?: ReportingDateDim(null, date).also {
            reportingDateDimMap[date] = it
        }

    fun getStatusDim(status: String): StatusDim =
        statusDimMap[status] ?: StatusDim(null, status).also {
            statusDimMap[status] = it
        }

    fun getDiseaseDim(disease: String): DiseaseDim =
        diseaseDimMap[disease] ?: DiseaseDim(null, disease).also {
            diseaseDimMap[disease] = it
        }

    fun getSerotypeDim(serotype: String): SerotypeDim =
        serotypeDimMap[serotype] ?: SerotypeDim(null, serotype).also {
            serotypeDimMap[serotype] = it
        }

    fun getSpeciesDim(species: String): SpeciesDim =
        speciesDimMap[species] ?: SpeciesDim(null, species).also {
            speciesDimMap[species] = it
        }

    fun getSourceDim(source: String): SourceDim =
        sourceDimMap[source] ?: SourceDim(null, source).also {
            sourceDimMap[source] = it
        }
}
