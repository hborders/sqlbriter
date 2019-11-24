package com.stealthmountain.sqldim;

import androidx.annotation.NonNull;

/**
 * {@link io.reactivex.functions.Function} doesn't have an annotation on its return type
 * because Java 7 doesn't support type-level annotations, and RxJava doesn't want to
 * have a bunch of combinatorial interfaces clogging things up.
 * We only need two such interfaces, so it's not too cluttered.
 *
 * O means optional, R means required. Thus, FunctionRR means both type parameters are
 * required (or {@link NonNull}).
 */
public interface FunctionRR<T, R> {
    @NonNull R applyRR(@NonNull T t) throws Exception;
}
