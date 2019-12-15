package com.dichotome.etl.source

import java.nio.file.Paths

sealed class Source {
    companion object {
        val ROOT_PATH = "${Paths.get("").toAbsolutePath()}\\src\\main\\resources\\csv"
    }

    abstract val path: String

    object DiseasesCases : Source() {
        override val path = "$ROOT_PATH\\disease_cases.csv"

        object Schema {
            const val id = "id"
            const val latitude = "latitude"
            const val longitude = "longitude"
            const val region = "region"
            const val country = "country"
            const val admin1 = "admin1"
            const val localityname = "localityname"
            const val localityquality = "localityquality"
            const val observationdate = "observationdate"
            const val reportingdate = "reportingdate"
            const val status = "status"
            const val disease = "disease"
            const val serotypes = "serotypes"
            const val speciesdescription = "speciesdescription"
            const val source = "source"
        }
    }

    object AnimalsInfo : Source() {
        override val path = "$ROOT_PATH\\animals_info.csv"

        object Schema {
            const val id = "id"
            const val sumatrisk = "sumatrisk"
            const val sumcases = "sumcases"
            const val sumdeaths = "sumdeaths"
            const val sumdestroyed = "sumdestroyed"
            const val sumslaughtered = "sumslaughtered"
        }
    }

    object HumanVictimsInfo : Source() {
        override val path = "$ROOT_PATH\\human_victims_info.csv"

        object Schema {
            const val id = "id"
            const val humansgenderdesc = "humansgenderdesc"
            const val humansage = "humansage"
            const val humansaffected = "humansaffected"
            const val humansdeaths = "humansdeaths"
        }
    }

    object DiseaseInfo : Source() {
        override val path = "$ROOT_PATH\\disease_info.csv"

        object Schema {
            const val name = "name"
            const val type = "type"
            const val target = "target"
            const val infects_humans = "infects_humans"
        }
    }
}
