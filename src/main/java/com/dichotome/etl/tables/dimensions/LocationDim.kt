package com.dichotome.etl.tables.dimensions

class LocationDim (
    var id: Int? = null,
    val latitude: Double,
    val longitude: Double,
    val continent: String,
    val country: String,
    val countryRegion: String,
    val localityName: String
)