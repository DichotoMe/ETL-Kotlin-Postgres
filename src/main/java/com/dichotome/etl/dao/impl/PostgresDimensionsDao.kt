package com.dichotome.etl.dao.impl

import com.dichotome.etl.dao.DimensionsDao
import com.dichotome.etl.dao.exception.DaoException
import com.dichotome.etl.db.ConnectionPool
import com.dichotome.etl.tables.dimensions.*
import com.dichotome.etl.utils.DateUtils
import java.sql.Date
import java.sql.SQLException
import java.sql.Statement
import java.util.*

class PostgresDimensionsDao : DimensionsDao {

    companion object {
        private const val INSERT_INTO_LOCATION_DIM = "insert into location_dim " +
            "(latitude, longitude, continent, country, country_region, locality_name) " +
            "values (?, ?, ?, ?, ?, ?)"

        private const val INSERT_INTO_OBSERVATION_DATE_DIM = "insert into observation_date_dim(date) values (?)"

        private const val INSERT_INTO_REPORTING_DATE_DIM = "insert into reporting_date_dim(date) values (?)"

        private const val INSERT_INTO_STATUS_DIM = "insert into status_dim(status) values (?)"

        private const val INSERT_INTO_DISEASE_DIM = "insert into disease_dim" +
            "(disease, type, target, infects_humans) " +
            "values (?,?,?,?)"

        private const val INSERT_INTO_SEROTYPE_DIM = "insert into serotype_dim(serotype) values (?)"

        private const val INSERT_INTO_SPECIES_DIM = "insert into species_dim(species) values (?)"

        private const val INSERT_INTO_SOURCE_DIM = "insert into source_dim(source) values (?)"
    }

    private fun isUniqueConstraintException(e: SQLException) = e.sqlState == "23505"

    private fun getIdOfExistingDimensionName(
        idColumnName: String,
        dimTableName: String,
        whereCondition: String
    ): Int {
        try {
            ConnectionPool.connection.use { conn ->
                val selectIdQuery = "select $idColumnName from $dimTableName where $whereCondition"
                conn.prepareStatement(selectIdQuery).use { preparedStatement ->
                    val resultSet = preparedStatement.executeQuery()

                    return if (resultSet.next()) {
                        resultSet.getInt(1)
                    } else {
                        throw NoSuchElementException("Cant find id of $dimTableName where $whereCondition")
                    }
                }
            }
        } catch (e: SQLException) {
            throw DaoException(e.message)
        }

    }

    override suspend fun createLocationDim(locationDim: LocationDim): Int {
        try {
            ConnectionPool.connection.use { conn ->
                conn.prepareStatement(
                    INSERT_INTO_LOCATION_DIM,
                    Statement.RETURN_GENERATED_KEYS
                ).use {
                    it.setDouble(1, locationDim.latitude)
                    it.setDouble(2, locationDim.longitude)
                    it.setString(3, locationDim.continent)
                    it.setString(4, locationDim.country)
                    it.setString(5, locationDim.countryRegion)
                    it.setString(6, locationDim.localityName)

                    it.executeUpdate()

                    return it.generatedKeys.takeIf { keys ->
                        keys.next()
                    }?.getInt("id") ?: throw DaoException("No keys generated for ${locationDim::class}")
                }
            }
        } catch (e: SQLException) {
            if (isUniqueConstraintException(e)) {
                return getIdOfExistingDimensionName(
                    "id",
                    "location_dim",
                    "latitude = ${locationDim.latitude} AND longitude = ${locationDim.longitude}"
                )
            } else throw DaoException(e.message)
        }
    }

    override suspend fun createObservationDateDim(observationDateDim: ObservationDateDim): Int {
        try {
            ConnectionPool.connection.use { conn ->
                conn.prepareStatement(
                    INSERT_INTO_OBSERVATION_DATE_DIM,
                    Statement.RETURN_GENERATED_KEYS
                ).use {
                    it.setDate(1, Date.valueOf(DateUtils.toSqlDateFormat(observationDateDim.date)))

                    it.executeUpdate()

                    return it.generatedKeys.takeIf { keys ->
                        keys.next()
                    }?.getInt("id") ?: throw DaoException("No keys generated for ${observationDateDim::class}")
                }
            }
        } catch (e: SQLException) {
            if (isUniqueConstraintException(e)) {
                return getIdOfExistingDimensionName(
                    "id",
                    "observation_date_dim",
                    "date = to_date('${observationDateDim.date}','DD/MM/YYYY')"
                )
            } else throw DaoException(e.message)
        }
    }

    override suspend fun createReportingDateDim(reportingDateDim: ReportingDateDim): Int {
        try {
            ConnectionPool.connection.use { conn ->
                conn.prepareStatement(
                    INSERT_INTO_REPORTING_DATE_DIM,
                    Statement.RETURN_GENERATED_KEYS
                ).use {
                    it.setDate(1, Date.valueOf(DateUtils.toSqlDateFormat(reportingDateDim.date)))

                    it.executeUpdate()

                    return it.generatedKeys.takeIf { keys ->
                        keys.next()
                    }?.getInt("id") ?: throw DaoException("No keys generated for ${reportingDateDim::class}")
                }
            }
        } catch (e: SQLException) {
            if (isUniqueConstraintException(e)) {
                return getIdOfExistingDimensionName(
                    "id",
                    "reporting_date_dim",
                    "date = to_date('${reportingDateDim.date}','DD/MM/YYYY')"
                )
            } else throw DaoException(e.message)
        }
    }

    override suspend fun createStatusDim(statusDim: StatusDim): Int {
        try {
            ConnectionPool.connection.use { conn ->
                conn.prepareStatement(
                    INSERT_INTO_STATUS_DIM,
                    Statement.RETURN_GENERATED_KEYS
                ).use {
                    it.setString(1, statusDim.status)

                    it.executeUpdate()

                    return it.generatedKeys.takeIf { keys ->
                        keys.next()
                    }?.getInt("id") ?: throw DaoException("No keys generated for ${statusDim::class}")
                }
            }
        } catch (e: SQLException) {
            if (isUniqueConstraintException(e)) {
                return getIdOfExistingDimensionName(
                    "id",
                    "status_dim",
                    "status = '${statusDim.status}'"
                )
            } else throw DaoException(e.message)
        }
    }

    override suspend fun createDiseaseDim(diseaseDim: DiseaseDim): Int {
        try {
            ConnectionPool.connection.use { conn ->
                conn.prepareStatement(
                    INSERT_INTO_DISEASE_DIM,
                    Statement.RETURN_GENERATED_KEYS
                ).use {
                    it.setString(1, diseaseDim.disease)
                    it.setString(2, diseaseDim.type)
                    it.setString(3, diseaseDim.target)
                    it.setString(4, diseaseDim.infectsHumans)

                    it.executeUpdate()

                    return it.generatedKeys.takeIf { keys ->
                        keys.next()
                    }?.getInt("id") ?: throw DaoException("No keys generated for ${diseaseDim::class}")
                }
            }
        } catch (e: SQLException) {
            if (isUniqueConstraintException(e)) {
                return getIdOfExistingDimensionName(
                    "id",
                    "disease_dim",
                    "disease = '${diseaseDim.disease}'"
                )
            } else throw DaoException(e.message)
        }
    }

    override suspend fun createSerotypeDim(serotypeDim: SerotypeDim): Int {
        try {
            ConnectionPool.connection.use { conn ->
                conn.prepareStatement(
                    INSERT_INTO_SEROTYPE_DIM,
                    Statement.RETURN_GENERATED_KEYS
                ).use {
                    it.setString(1, serotypeDim.serotype)

                    it.executeUpdate()

                    return it.generatedKeys.takeIf { keys ->
                        keys.next()
                    }?.getInt("id") ?: throw DaoException("No keys generated for ${serotypeDim::class}")
                }
            }
        } catch (e: SQLException) {
            if (isUniqueConstraintException(e)) {
                return getIdOfExistingDimensionName(
                    "id",
                    "serotype_dim",
                    "serotype = '${serotypeDim.serotype}'"
                )
            } else throw DaoException(e.message)
        }
    }

    override suspend fun createSpeciesDim(speciesDim: SpeciesDim): Int {
        try {
            ConnectionPool.connection.use { conn ->
                conn.prepareStatement(
                    INSERT_INTO_SPECIES_DIM,
                    Statement.RETURN_GENERATED_KEYS
                ).use {
                    it.setString(1, speciesDim.species)

                    it.executeUpdate()

                    return it.generatedKeys.takeIf { keys ->
                        keys.next()
                    }?.getInt("id") ?: throw DaoException("No keys generated for ${speciesDim::class}")
                }
            }
        } catch (e: SQLException) {
            if (isUniqueConstraintException(e)) {
                return getIdOfExistingDimensionName(
                    "id",
                    "species_dim",
                    "species = '${speciesDim.species}'"
                )
            } else throw DaoException(e.message)
        }
    }

    override suspend fun createSourceDim(sourceDim: SourceDim): Int {
        try {
            ConnectionPool.connection.use { conn ->
                conn.prepareStatement(
                    INSERT_INTO_SOURCE_DIM,
                    Statement.RETURN_GENERATED_KEYS
                ).use {
                    it.setString(1, sourceDim.source)

                    it.executeUpdate()

                    return it.generatedKeys.takeIf { keys ->
                        keys.next()
                    }?.getInt("id") ?: throw DaoException("No keys generated for ${sourceDim::class}")
                }
            }
        } catch (e: SQLException) {
            if (isUniqueConstraintException(e)) {
                return getIdOfExistingDimensionName(
                    "id",
                    "source_dim",
                    "source = '${sourceDim.source}'"
                )
            } else throw DaoException(e.message)
        }
    }
}