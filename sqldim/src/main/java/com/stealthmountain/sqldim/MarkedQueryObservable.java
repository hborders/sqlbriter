package com.stealthmountain.sqldim;

import android.database.Cursor;
import android.os.Build;

import androidx.annotation.CheckResult;
import androidx.annotation.NonNull;
import androidx.annotation.RequiresApi;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import io.reactivex.Observable;
import io.reactivex.Observer;
import io.reactivex.functions.BiFunction;
import io.reactivex.functions.Function;

import static com.stealthmountain.sqldim.SqlDim.MarkedQuery;
import static com.stealthmountain.sqldim.SqlDim.MarkedQuery.MarkedValue;

/** An {@link Observable} of {@link MarkedQuery} which offers query-specific convenience operators. */
public final class MarkedQueryObservable<M> extends Observable<MarkedQuery<M>> {
  /** This can't be a {@link FunctionRR} because it has to interact directly with RxJava */
  @NonNull static <M> Function<Observable<MarkedQuery<M>>, MarkedQueryObservable<M>> markedQueryObserable() {
    return new Function<Observable<MarkedQuery<M>>, MarkedQueryObservable<M>>() {
      @NonNull
      @Override
      public MarkedQueryObservable<M> apply(@NonNull Observable<MarkedQuery<M>> queryObservable) {
        return new MarkedQueryObservable<>(queryObservable);
      }
    };
  }

  @NonNull private final Observable<MarkedQuery<M>> upstream;

  public MarkedQueryObservable(@NonNull Observable<MarkedQuery<M>> upstream) {
    this.upstream = upstream;
  }

  @Override protected void subscribeActual(@NonNull Observer<? super MarkedQuery<M>> observer) {
    upstream.subscribe(observer);
  }

  /**
   * Given a function mapping the current row of a {@link Cursor} and the marker to {@code T}, transform each
   * emitted {@link MarkedQuery} which returns a single row to {@code T}.
   * <p>
   * It is an error for a query to pass through this operator with more than 1 row in its result
   * set. Use {@code LIMIT 1} on the underlying SQL query to prevent this. Result sets with 0 rows
   * do not emit an item.
   * <p>
   * This method is equivalent to:
   * <pre>{@code
   * flatMap(q -> q.asRows(mapper).take(1))
   * }</pre>
   * and a convenience operator for:
   * <pre>{@code
   * lift(Query.mapToOne(mapper))
   * }</pre>
   *
   * @param mapper Maps the current {@link Cursor} and the marker row to {@code T}. May not return null.
   */
  @CheckResult @NonNull
  public final <T> Observable<MarkedValue<M, T>> mapToOne(@NonNull BiFunction<Cursor, Set<M>, T> mapper) {
    return lift(MarkedQuery.mapToOne(mapper));
  }

  /**
   * Given a function mapping the current row of a {@link Cursor} and the marker to {@code T}, transform each
   * emitted {@link MarkedQuery} which returns a single row to {@code T}.
   * <p>
   * It is an error for a query to pass through this operator with more than 1 row in its result
   * set. Use {@code LIMIT 1} on the underlying SQL query to prevent this. Result sets with 0 rows
   * emit {@code defaultValue}.
   * <p>
   * This method is equivalent to:
   * <pre>{@code
   * flatMap(q -> q.asRows(mapper).take(1).defaultIfEmpty(defaultValue))
   * }</pre>
   * and a convenience operator for:
   * <pre>{@code
   * lift(Query.mapToOneOrDefault(mapper, defaultValue))
   * }</pre>
   *
   * @param mapper Maps the current {@link Cursor} and the marker row to {@code T}. May not return null.
   * @param defaultValue Value returned if result set is empty
   */
  @CheckResult @NonNull
  public final <T> Observable<MarkedValue<M, T>> mapToOneOrDefault(@NonNull BiFunction<Cursor, Set<M>, T> mapper,
      @NonNull T defaultValue) {
    return lift(MarkedQuery.mapToOneOrDefault(mapper, defaultValue));
  }

  /**
   * Given a function mapping the current row of a {@link Cursor} and the marker to {@code T}, transform each
   * emitted {@link MarkedQuery} which returns a single row to {@code Optional<T>}.
   * <p>
   * It is an error for a query to pass through this operator with more than 1 row in its result
   * set. Use {@code LIMIT 1} on the underlying SQL query to prevent this. Result sets with 0 rows
   * emit {@link Optional#empty() Optional.empty()}
   * <p>
   * This method is equivalent to:
   * <pre>{@code
   * flatMap(q -> q.asRows(mapper).take(1).map(Optional::of).defaultIfEmpty(Optional.empty())
   * }</pre>
   * and a convenience operator for:
   * <pre>{@code
   * lift(Query.mapToOptional(mapper))
   * }</pre>
   *
   * @param mapper Maps the current {@link Cursor} row and the marker to {@code T}. May not return null.
   */
  @RequiresApi(Build.VERSION_CODES.N)
  @CheckResult @NonNull
  public final <T> Observable<MarkedValue<M, Optional<T>>> mapToOptional(@NonNull BiFunction<Cursor, Set<M>, T> mapper) {
    return lift(MarkedQuery.mapToOptional(mapper));
  }

  /**
   * Given a function mapping the current row of a {@link Cursor} and the marker to {@code T}, transform each
   * emitted {@link MarkedQuery} to a {@code List<T>}.
   * <p>
   * Be careful using this operator as it will always consume the entire cursor and create objects
   * for each row, every time this observable emits a new query. On tables whose queries update
   * frequently or very large result sets this can result in the creation of many objects.
   * <p>
   * This method is equivalent to:
   * <pre>{@code
   * flatMap(q -> q.asRows(mapper).toList())
   * }</pre>
   * and a convenience operator for:
   * <pre>{@code
   * lift(Query.mapToList(mapper))
   * }</pre>
   * <p>
   * Consider using {@link MarkedQuery#asRows} if you need to limit or filter in memory.
   *
   * @param mapper Maps the current {@link Cursor} row and the marker to {@code T}. May not return null.
   */
  @CheckResult @NonNull
  public final <T> Observable<MarkedValue<M, List<T>>> mapToList(@NonNull BiFunction<Cursor, Set<M>, T> mapper) {
    return lift(MarkedQuery.mapToList(mapper));
  }
}
