package com.dichotome.etl.utils

object DateUtils {
    val date_regex = Regex("\\d{1,2}/\\d{1,2}/\\d{4}")

    fun toSqlDateFormat(date: String) =
        date.split('/').let { (day, month, year) ->
            "$year-$month-$day"
        }

    fun validate(date: String) =
        date.takeIf { date_regex.matches(it) } ?: throw IllegalArgumentException("Invalid date format $date")
}