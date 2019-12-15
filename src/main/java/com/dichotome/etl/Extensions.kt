package com.dichotome.etl

import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch

inline fun <T> guard(riskyTask: () -> T) =
    try {
        riskyTask.invoke()
    } catch (e: Exception) {
        e.printStackTrace()
        null
    }

inline fun <T, S> guard(scope: S, riskyTask: S.() -> T) =
    try {
        riskyTask.invoke(scope)
    } catch (e: Exception) {
        e.printStackTrace(System.out)
        null
    }


fun String?.toIntOrZero() = takeIf { !it.isNullOrBlank() }?.toInt() ?: 0

suspend fun <T> Collection<T>.forEachParallel(action: suspend (T) -> Unit) = coroutineScope {
    forEach { launch { action(it) } }
}