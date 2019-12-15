package com.dichotome.etl.data.mappers

import com.dichotome.etl.guard
import com.dichotome.etl.source.Source
import com.dichotome.etl.tables.Fact
import com.dichotome.etl.toIntOrZero
import com.dichotome.etl.utils.DateUtils
import com.dichotome.etl.utils.DimensionsStage
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.CSVParser
import java.io.File
import java.io.FileNotFoundException
import java.nio.charset.StandardCharsets


object DataParser {

    private val parsers = hashMapOf(
        Source.DiseasesCases to createParser(Source.DiseasesCases),
        Source.AnimalsInfo to createParser(Source.AnimalsInfo),
        Source.HumanVictimsInfo to createParser(Source.HumanVictimsInfo)
    )

    private fun createParser(source: Source): CSVParser =
        if (File(source.path).exists()) {
            CSVParser.parse(
                File(source.path),
                StandardCharsets.UTF_8,
                CSVFormat
                    .newFormat(',')
                    .withQuote('"')
                    .withFirstRecordAsHeader()
                    .withIgnoreSurroundingSpaces()
            )
        } else {
            throw FileNotFoundException("File not found ${source.path}")
        }


    fun parse(
        diseaseCasesSrc: Source.DiseasesCases,
        animalsSrc: Source.AnimalsInfo,
        humansSrc: Source.HumanVictimsInfo,
        diseaseInfoSrc: Source.DiseaseInfo
    ): List<Fact> = runBlocking {
        val diseaseCasesParserDef = async { createParser(diseaseCasesSrc) }
        val animalsParserDef = async { createParser(animalsSrc) }
        val humanParserDef = async { createParser(humansSrc) }
        val diseaseInfoParserDef = async { createParser(diseaseInfoSrc) }

        val diseaseModels = diseaseCasesParserDef.await().map {
            DataMapper.diseasesModelMapper(it)
        }.associateBy { it.id }

        val animalModels = animalsParserDef.await().map {
            DataMapper.animalsModelMapper(it)
        }.associateBy { it.id }

        val humanModels = humanParserDef.await().map {
            DataMapper.humansModelMapper(it)
        }.associateBy { it.id }

        val diseaseInfoModels = diseaseInfoParserDef.await().map {
            DataMapper.diseaseInfoModelMapper(it)
        }.associateBy { it.name }

        diseaseModels.keys.mapNotNull { id ->
            val diseaseCasesItem = diseaseModels[id] ?: return@mapNotNull null
            val animalsItem = animalModels[id] ?: return@mapNotNull null
            val humansItem = humanModels[id] ?: return@mapNotNull null
            val diseaseInfoItem = diseaseInfoModels[diseaseCasesItem.disease] ?: return@mapNotNull null

            guard(DimensionsStage) {
                Fact(
                    id.toInt(),
                    getLocationDim(
                        diseaseCasesItem.latitude.toDouble(),
                        diseaseCasesItem.longitude.toDouble(),
                        diseaseCasesItem.continent,
                        diseaseCasesItem.country,
                        diseaseCasesItem.countryRegion,
                        diseaseCasesItem.localityname
                    ),
                    getObservationDateDim(DateUtils.validate(diseaseCasesItem.observationDate)),
                    getReportingDateDim(DateUtils.validate(diseaseCasesItem.reportingDate)),
                    getStatusDim(diseaseCasesItem.status),
                    getDiseaseDim(
                        diseaseCasesItem.disease,
                        diseaseInfoItem.type,
                        diseaseInfoItem.target,
                        diseaseInfoItem.infectsHumans
                    ),
                    getSerotypeDim(diseaseCasesItem.serotype),
                    getSpeciesDim(diseaseCasesItem.species),
                    getSourceDim(diseaseCasesItem.source),
                    animalsItem.animalsAtRisk.toIntOrZero(),
                    animalsItem.animalsAffected.toIntOrZero(),
                    animalsItem.animalsDeaths.toIntOrZero(),
                    animalsItem.animalsDestroyed.toIntOrZero(),
                    animalsItem.animalsSlaughtered.toIntOrZero(),
                    humansItem.humansAffected.toIntOrZero(),
                    humansItem.humansDeaths.toIntOrZero()
                )
            }
        }
    }
}