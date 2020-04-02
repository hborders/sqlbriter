package com.stealthmountain.sqldim;

import androidx.annotation.NonNull;

/**
 * A functional interface (callback) that computes a value based on multiple input values.
 *
 * checkerframework doesn't pick up io.reactivex.rxjava3.annotations.NonNull as a generic annotation yet,
 * so we have to use this. https://github.com/typetools/checker-framework/pull/3222
 *
 * @param <T1> the first value type
 * @param <T2> the second value type
 * @param <R> the result type
 */
@FunctionalInterface
public interface NonNullBiFunction<T1, T2, R> {

    /**
     * Calculate a value based on the input values.
     * @param t1 the first value
     * @param t2 the second value
     * @return the result value
     * @throws Throwable if the implementation wishes to throw any type of exception
     */
    @NonNull R apply(@NonNull T1 t1, @NonNull T2 t2) throws Throwable;
}
