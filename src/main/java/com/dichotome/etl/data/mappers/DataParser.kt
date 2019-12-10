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
            )
        } else {
            throw FileNotFoundException("File not found ${source.path}")
        }


    fun parse(
        diseasesSrc: Source.DiseasesCases,
        animalsSrc: Source.AnimalsInfo,
        humansSrc: Source.HumanVictimsInfo
    ): List<Fact> = runBlocking {
        val diseaseParserDef = async { createParser(diseasesSrc) }
        val animalsParserDef = async { createParser(animalsSrc) }
        val humanParserDef = async { createParser(humansSrc) }

        val diseaseModels = diseaseParserDef.await().map {
            DataMapper.diseasesModelMapper(it)
        }.associateBy { it.id }

        val animalModels = animalsParserDef.await().map {
            DataMapper.animalsModelMapper(it)
        }.associateBy { it.id }

        val humanModels = humanParserDef.await().map {
            DataMapper.humansModelMapper(it)
        }.associateBy { it.id }

        diseaseModels.keys.filter { id ->
            animalModels.containsKey(id) && humanModels.containsKey(id)
        }.mapNotNull { id ->
            val diseaseItem = diseaseModels[id]!!
            val animalsItem = animalModels[id]!!
            val humansItem = humanModels[id]!!

            guard(DimensionsStage) {
                Fact(
                    id.toInt(),
                    getLocationDim(
                        diseaseItem.latitude.toDouble(),
                        diseaseItem.longitude.toDouble(),
                        diseaseItem.region,
                        diseaseItem.country,
                        diseaseItem.localityname
                    ),
                    getObservationDateDim(DateUtils.validate(diseaseItem.observationDate)),
                    getReportingDateDim(DateUtils.validate(diseaseItem.reportingDate)),
                    getStatusDim(diseaseItem.status),
                    getDiseaseDim(diseaseItem.disease),
                    getSerotypeDim(diseaseItem.serotype),
                    getSpeciesDim(diseaseItem.species),
                    getSourceDim(diseaseItem.source),
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