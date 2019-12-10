package com.dichotome.etl.tables.dimensions

class LocationDim (
    var id: Int? = null,
    val latitude: Double,
    val longitude: Double,
    val region: String,
    val country: String,
    val localityName: String
)