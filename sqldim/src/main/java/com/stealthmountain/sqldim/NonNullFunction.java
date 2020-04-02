package com.stealthmountain.sqldim;

import androidx.annotation.NonNull;

/**
 * A functional interface that takes a value and returns another value, possibly with a
 * different type and allows throwing a checked exception.
 *
 * checkerframework doesn't pick up io.reactivex.rxjava3.annotations.NonNull as a generic annotation yet,
 * so we have to use this. https://github.com/typetools/checker-framework/pull/3222
 *
 * @param <T> the input value type
 * @param <R> the output value type
 */
@FunctionalInterface
public interface NonNullFunction<T, R> {
    /**
     * Apply some calculation to the input value and return some other value.
     * @param t the input value
     * @return the output value
     * @throws Throwable if the implementation wishes to throw any type of exception
     */
    @NonNull R apply(@NonNull T t) throws Throwable;
}
