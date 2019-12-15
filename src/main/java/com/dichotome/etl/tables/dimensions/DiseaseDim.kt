package com.dichotome.etl.tables.dimensions

data class DiseaseDim (
    var id: Int? = null,
    val disease: String,
    val type: String,
    val target: String,
    val infectsHumans: String
)