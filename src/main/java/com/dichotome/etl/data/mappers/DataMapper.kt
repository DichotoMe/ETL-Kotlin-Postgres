package com.dichotome.etl.data.mappers

import com.dichotome.etl.source.Source
import com.dichotome.etl.tables.partialFacts.AnimalsFactModel
import com.dichotome.etl.tables.partialFacts.DiseaseFactModel
import com.dichotome.etl.tables.partialFacts.HumansFactModel
import org.apache.commons.csv.CSVRecord

object DataMapper {
    val diseasesModelMapper = fun(record: CSVRecord): DiseaseFactModel {
        with(Source.DiseasesCases.Schema) {
            return DiseaseFactModel(
                record[id],
                record[latitude],
                record[longitude],
                record[region],
                record[country],
                record[localityname],
                record[observationdate],
                record[reportingdate],
                record[status],
                record[disease],
                record[serotypes],
                record[speciesdescription],
                record[source]
            )
        }
    }

    val animalsModelMapper = fun(record: CSVRecord): AnimalsFactModel {
        with(Source.AnimalsInfo.Schema) {
            return AnimalsFactModel(
                record[id],
                record[sumatrisk],
                record[sumcases],
                record[sumdeaths],
                record[sumdestroyed],
                record[sumslaughtered]
            )
        }
    }

    val humansModelMapper = fun(record: CSVRecord): HumansFactModel {
        with(Source.HumanVictimsInfo.Schema) {
            return HumansFactModel(
                record[id],
                record[humansaffected],
                record[humansdeaths]
            )
        }
    }
}