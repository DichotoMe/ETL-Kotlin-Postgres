package com.dichotome.etl.dao.impl

import com.dichotome.etl.dao.DaoFactory
import com.dichotome.etl.dao.DimensionsDao
import com.dichotome.etl.dao.FactDao

object PostgresDaoFactory : DaoFactory {
    override fun createFactDao(): FactDao = PostgresFactDao()

    override fun createDimensionDao(): DimensionsDao = PostgresDimensionsDao()
}