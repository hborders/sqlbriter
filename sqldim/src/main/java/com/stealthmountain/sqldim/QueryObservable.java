package com.stealthmountain.sqldim;

import android.database.Cursor;
import android.os.Build;
import androidx.annotation.CheckResult;
import androidx.annotation.NonNull;
import androidx.annotation.RequiresApi;
import com.stealthmountain.sqldim.SqlDim.Query;
import java.util.List;
import java.util.Optional;

import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.ObservableConverter;
import io.reactivex.rxjava3.core.Observer;
import io.reactivex.rxjava3.functions.Function;

/** An {@link Observable} of {@link Query} which offers query-specific convenience operators. */
public final class QueryObservable extends Observable<Query> {
  @NonNull static final ObservableConverter<Query, QueryObservable> QUERY_OBSERVABLE =
      new ObservableConverter<Query, QueryObservable>() {
        @NonNull @Override public QueryObservable apply(@NonNull Observable<Query> queryObservable) {
          return new QueryObservable(queryObservable);
        }
      };

  @NonNull private final Observable<Query> upstream;

  public QueryObservable(@NonNull Observable<Query> upstream) {
    this.upstream = upstream;
  }

  @Override protected void subscribeActual(@NonNull Observer<? super Query> observer) {
    upstream.subscribe(observer);
  }

  /**
   * Given a function mapping the current row of a {@link Cursor} to {@code T}, transform each
   * emitted {@link Query} which returns a single row to {@code T}.
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
   * @param mapper Maps the current {@link Cursor} row to {@code T}. May not return null.
   */
  @CheckResult @NonNull
  public final <T> Observable<T> mapToOne(@NonNull Function<Cursor, T> mapper) {
    return lift(Query.mapToOne(mapper));
  }

  /**
   * Given a function mapping the current row of a {@link Cursor} to {@code T}, transform each
   * emitted {@link Query} which returns a single row to {@code T}.
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
   * @param mapper Maps the current {@link Cursor} row to {@code T}. May not return null.
   * @param defaultValue Value returned if result set is empty
   */
  @CheckResult @NonNull
  public final <T> Observable<T> mapToOneOrDefault(@NonNull Function<Cursor, T> mapper,
      @NonNull T defaultValue) {
    return lift(Query.mapToOneOrDefault(mapper, defaultValue));
  }

  /**
   * Given a function mapping the current row of a {@link Cursor} to {@code T}, transform each
   * emitted {@link Query} which returns a single row to {@code Optional<T>}.
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
   * @param mapper Maps the current {@link Cursor} row to {@code T}. May not return null.
   */
  @RequiresApi(Build.VERSION_CODES.N)
  @CheckResult @NonNull
  public final <T> Observable<Optional<T>> mapToOptional(@NonNull Function<Cursor, T> mapper) {
    return lift(Query.mapToOptional(mapper));
  }

  /**
   * Given a function mapping the current row of a {@link Cursor} to {@code T}, transform each
   * emitted {@link Query} to a {@code List<T>}.
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
   * Consider using {@link Query#asRows} if you need to limit or filter in memory.
   *
   * @param mapper Maps the current {@link Cursor} row to {@code T}. May not return null.
   */
  @CheckResult @NonNull
  public final <T> Observable<List<T>> mapToList(@NonNull Function<Cursor, T> mapper) {
    return lift(Query.mapToList(mapper));
  }

  /**
   * Given a function mapping the current row of a {@link Cursor} to {@code T}, transform each
   * emitted {@link Query} to a {@code List<T>}.
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
   * Consider using {@link Query#asRows} if you need to limit or filter in memory.
   *
   * @param mapper Maps the current {@link Cursor} row to {@code T}. May not return null.
   * @param newList Creates the new list. May not return null.
   */
  @CheckResult @NonNull
  public final <L extends List<T>, T> Observable<L> mapToSpecificList(
          @NonNull Function<Cursor, T> mapper, @NonNull NewList<L, T> newList) {
    return lift(Query.mapToSpecificList(mapper, newList));
  }
}
