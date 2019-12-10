package com.dichotome.etl.dao

interface DaoFactory {
    fun createFactDao(): FactDao

    fun createDimensionDao(): DimensionsDao
}
