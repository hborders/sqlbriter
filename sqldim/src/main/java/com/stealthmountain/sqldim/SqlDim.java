/*
 * Copyright (C) 2015 Square, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.stealthmountain.sqldim;

import androidx.sqlite.db.SupportSQLiteOpenHelper;
import android.content.ContentResolver;
import android.database.Cursor;
import android.os.Build;
import androidx.annotation.CheckResult;
import androidx.annotation.NonNull;
import androidx.annotation.Nullable;
import androidx.annotation.RequiresApi;
import androidx.annotation.WorkerThread;
import android.util.Log;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.ObservableEmitter;
import io.reactivex.rxjava3.core.ObservableOnSubscribe;
import io.reactivex.rxjava3.core.ObservableOperator;
import io.reactivex.rxjava3.core.ObservableSource;
import io.reactivex.rxjava3.core.ObservableTransformer;
import io.reactivex.rxjava3.core.Scheduler;
import io.reactivex.rxjava3.functions.BiFunction;
import io.reactivex.rxjava3.functions.Function;

/**
 * A lightweight wrapper around {@link SupportSQLiteOpenHelper} which allows for continuously
 * observing the result of a query.
 */
public final class SqlDim<M> {
  @NonNull static final Logger DEFAULT_LOGGER = new Logger() {
    @Override public void log(@NonNull String message) {
      Log.d("SqlDim", message);
    }
  };
  @NonNull static final ObservableTransformer<Query, Query> DEFAULT_TRANSFORMER =
      new ObservableTransformer<Query, Query>() {
        @NonNull @Override public Observable<Query> apply(@NonNull Observable<Query> queryObservable) {
          return queryObservable;
        }
      };

  public static final class Builder<M> {
    @NonNull private Logger logger = DEFAULT_LOGGER;
    @NonNull private ObservableTransformer<Query, Query> queryTransformer = DEFAULT_TRANSFORMER;
    @NonNull private ObservableTransformer<MarkedQuery<M>, MarkedQuery<M>> markedQueryTransformer =
            new ObservableTransformer<MarkedQuery<M>, MarkedQuery<M>>() {
              @NonNull @Override
              public ObservableSource<MarkedQuery<M>> apply(Observable<MarkedQuery<M>> markedQueryObservable) {
                return markedQueryObservable;
              }
            };

    @CheckResult @NonNull
    public Builder logger(@NonNull Logger logger) {
      if (logger == null) throw new NullPointerException("logger == null");
      this.logger = logger;
      return this;
    }

    @CheckResult @NonNull
    public Builder queryTransformer(@NonNull ObservableTransformer<Query, Query> queryTransformer) {
      if (queryTransformer == null) throw new NullPointerException("queryTransformer == null");
      this.queryTransformer = queryTransformer;
      return this;
    }

    @CheckResult @NonNull
    public Builder markedQueryTransformer(@NonNull ObservableTransformer<MarkedQuery<M>, MarkedQuery<M>> markedQueryTransformer) {
      if (markedQueryTransformer == null) throw new NullPointerException("markedQueryTransformer == null");
      this.markedQueryTransformer = markedQueryTransformer;
      return this;
    }

    @CheckResult @NonNull
    public SqlDim<M> build() {
      return new SqlDim<>(logger, queryTransformer, markedQueryTransformer);
    }
  }

  @NonNull final Logger logger;
  @NonNull final ObservableTransformer<Query, Query> queryTransformer;
  @NonNull final ObservableTransformer<MarkedQuery<M>, MarkedQuery<M>> markedQueryTransformer;

  SqlDim(@NonNull Logger logger,
         @NonNull ObservableTransformer<Query, Query> queryTransformer,
         @NonNull ObservableTransformer<MarkedQuery<M>, MarkedQuery<M>> markedQueryTransformer) {
    this.logger = logger;
    this.queryTransformer = queryTransformer;
    this.markedQueryTransformer = markedQueryTransformer;
  }

  /**
   * Wrap a {@link SupportSQLiteOpenHelper} for observable queries.
   * <p>
   * While not strictly required, instances of this class assume that they will be the only ones
   * interacting with the underlying {@link SupportSQLiteOpenHelper} and it is required for
   * automatic notifications of table changes to work. See
   * {@linkplain DimDatabase#createQuery(String, String, Object...) the <code>query</code> method}
   * for more information on that behavior.
   *
   * @param scheduler The {@link Scheduler} on which items from
   * {@link DimDatabase#createQuery(String, String, Object...)} will be emitted.
   */
  @CheckResult @NonNull public DimDatabase<M> wrapDatabaseHelper(
      @NonNull SupportSQLiteOpenHelper helper,
      @NonNull Scheduler scheduler) {
    return new DimDatabase<M>(
            helper,
            logger,
            scheduler,
            queryTransformer,
            markedQueryTransformer
    );
  }

  /**
   * Wrap a {@link ContentResolver} for observable queries.
   *
   * @param scheduler The {@link Scheduler} on which items from
   * {@link DimContentResolver#createQuery} will be emitted.
   */
  @CheckResult @NonNull public DimContentResolver wrapContentProvider(
      @NonNull ContentResolver contentResolver, @NonNull Scheduler scheduler) {
    return new DimContentResolver(contentResolver, logger, scheduler, queryTransformer);
  }

  /**
   * An executable query with the markers of the operation that triggered the query.
   * Doesn't extend {@link Query} because we can't use {@link #equals} to compare
   * the two due to the symmetry principle.
   */
  public static abstract class MarkedQuery<M> {
    public static final class MarkedValue<M, V> {
      @NonNull public final Set<M> markers;
      @NonNull public final V value;

      public MarkedValue(@NonNull V value) {
        this(Collections.emptySet(), value);
      }

      public MarkedValue(@NonNull M marker, @NonNull V value) {
        this(Collections.singleton(marker), value);
      }

      public MarkedValue(@NonNull Set<M> markers, @NonNull V value) {
        this.markers = markers;
        this.value = value;
      }

      @Override
      public boolean equals(@Nullable Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MarkedValue<?, ?> that = (MarkedValue<?, ?>) o;

        if (!markers.equals(that.markers)) return false;
        return value.equals(that.value);
      }

      @Override
      public int hashCode() {
        int result = markers.hashCode();
        result = 31 * result + value.hashCode();
        return result;
      }

      @Override
      public String toString() {
        return "MarkedValue[" +
                "markers=" + markers +
                ", value=" + value +
                ']';
      }
    }

    @NonNull public final Set<M> markers;

    MarkedQuery(@NonNull Set<M> markers) {
      this.markers = markers;
    }
    /**
     * Creates an {@linkplain ObservableOperator operator} which transforms a query returning a
     * single row to a {@code T} using {@code mapper}. Use with {@link Observable#lift}.
     * <p>
     * It is an error for a query to pass through this operator with more than 1 row in its result
     * set. Use {@code LIMIT 1} on the underlying SQL query to prevent this. Result sets with 0 rows
     * do not emit an item.
     * <p>
     * This operator ignores {@code null} cursors returned from {@link #run()}.
     *
     * @param mapper Maps the current {@link Cursor} row and markers {@code M} to {@code T}. May not return null.
     */
    @CheckResult @NonNull //
    public static <M, T> ObservableOperator<MarkedValue<M, T>, MarkedQuery<M>> mapToOne(@NonNull BiFunction<Cursor, Set<M>, T> mapper) {
      return new MarkedQueryToOneOperator<>(mapper, null);
    }

    /**
     * Creates an {@linkplain ObservableOperator operator} which transforms a query returning a
     * single row to a {@code T} using {@code mapper}. Use with {@link Observable#lift}.
     * <p>
     * It is an error for a query to pass through this operator with more than 1 row in its result
     * set. Use {@code LIMIT 1} on the underlying SQL query to prevent this. Result sets with 0 rows
     * emit {@code defaultValue}.
     * <p>
     * This operator emits {@code defaultValue} if {@code null} is returned from {@link #run()}.
     *
     * @param mapper Maps the current {@link Cursor} row and markers {@code M} to {@code T}. May not return null.
     * @param defaultValue Value returned if result set is empty
     */
    @SuppressWarnings("ConstantConditions") // Public API contract.
    @CheckResult @NonNull
    public static <M, T> ObservableOperator<MarkedValue<M, T>, MarkedQuery<M>> mapToOneOrDefault(
            @NonNull BiFunction<Cursor, Set<M>, T> mapper, @NonNull T defaultValue) {
      if (defaultValue == null) throw new NullPointerException("defaultValue == null");
      return new MarkedQueryToOneOperator<>(mapper, defaultValue);
    }

    /**
     * Creates an {@linkplain ObservableOperator operator} which transforms a query returning a
     * single row to a {@code Optional<T>} using {@code mapper}. Use with {@link Observable#lift}.
     * <p>
     * It is an error for a query to pass through this operator with more than 1 row in its result
     * set. Use {@code LIMIT 1} on the underlying SQL query to prevent this. Result sets with 0 rows
     * emit {@link Optional#empty() Optional.empty()}.
     * <p>
     * This operator ignores {@code null} cursors returned from {@link #run()}.
     *
     * @param mapper Maps the current {@link Cursor} row and markers {@code M} to {@code T}. May not return null.
     */
    @RequiresApi(Build.VERSION_CODES.N) //
    @CheckResult @NonNull //
    public static <M, T> ObservableOperator<MarkedValue<M, Optional<T>>, MarkedQuery<M>> mapToOptional(
            @NonNull BiFunction<Cursor, Set<M>, T> mapper) {
      return new MarkedQueryToOptionalOperator<>(mapper);
    }

    /**
     * Creates an {@linkplain ObservableOperator operator} which transforms a query to a
     * {@code List<T>} using {@code mapper}. Use with {@link Observable#lift}.
     * <p>
     * Be careful using this operator as it will always consume the entire cursor and create objects
     * for each row, every time this observable emits a new query. On tables whose queries update
     * frequently or very large result sets this can result in the creation of many objects.
     * <p>
     * This operator ignores {@code null} cursors returned from {@link #run()}.
     *
     * @param mapper Maps the current {@link Cursor} row and markers {@code M} to {@code T}. May not return null.
     */
    @CheckResult @NonNull
    public static <M, T> ObservableOperator<MarkedValue<M, List<T>>, MarkedQuery<M>> mapToList(
        @NonNull BiFunction<Cursor, Set<M>, T> mapper) {
      return new MarkedQueryToListOperator<>(mapper, ArrayList::new);
    }

    /**
     * Creates an {@linkplain ObservableOperator operator} which transforms a query to a
     * {@code List<T>} using {@code mapper}. Use with {@link Observable#lift}.
     * <p>
     * Be careful using this operator as it will always consume the entire cursor and create objects
     * for each row, every time this observable emits a new query. On tables whose queries update
     * frequently or very large result sets this can result in the creation of many objects.
     * <p>
     * This operator ignores {@code null} cursors returned from {@link #run()}.
     *
     * @param mapper Maps the current {@link Cursor} row and markers {@code M} to {@code T}. May not return null.
     * @param newList Creates the new list. May not return null.
     */
    @CheckResult @NonNull
    public static <M, L extends List<T>, T> ObservableOperator<MarkedValue<M, L>, MarkedQuery<M>> mapToSpecificList(
        @NonNull BiFunction<Cursor, Set<M>, T> mapper, @NonNull NewList<L, T> newList) {
      return new MarkedQueryToListOperator<>(mapper, newList);
    }

    /**
     * final because we don't want subclasses to override or else, they'll violate
     * the symmetry equals/hashCode principle.
     */
    @Override
    public final int hashCode() {
      return super.hashCode();
    }

    /**
     * final because we don't want subclasses to override or else, they'll violate
     * the symmetry equals/hashCode principle.
     */
    @Override
    public final boolean equals(@Nullable Object obj) {
      return super.equals(obj);
    }

    /**
     * Just for the method reference, you should probably use {@link #markers}
     */
    @Nullable
    public Set<M> getMarkers() {
      return markers;
    }

    /**
     * Execute the query on the underlying database and return the resulting cursor.
     *
     * @return A {@link Cursor} with query results, or {@code null} when the query could not be
     * executed due to a problem with the underlying store. Unfortunately it is not well documented
     * when {@code null} is returned. It usually involves a problem in communicating with the
     * underlying store and should either be treated as failure or ignored for retry at a later
     * time.
     */
    @CheckResult @WorkerThread
    @Nullable
    public abstract Cursor run();

    /**
     * Execute the query on the underlying database and return an Observable of each row mapped to
     * {@code T} by {@code mapper}.
     * <p>
     * Standard usage of this operation is in {@code flatMap}:
     * <pre>{@code
     * flatMap(q -> q.asRows(Item.MAPPER).toList())
     * }</pre>
     * However, the above is a more-verbose but identical operation as
     * {@link QueryObservable#mapToList}. This {@code asRows} method should be used when you need
     * to limit or filter the items separate from the actual query.
     * <pre>{@code
     * flatMap(q -> q.asRows(Item.MAPPER).take(5).toList())
     * // or...
     * flatMap(q -> q.asRows(Item.MAPPER).filter(i -> i.isActive).toList())
     * }</pre>
     * <p>
     * Note: Limiting results or filtering will almost always be faster in the database as part of
     * a query and should be preferred, where possible.
     * <p>
     * The resulting observable will be empty if {@code null} is returned from {@link #run()}.
     */
    @CheckResult @NonNull
    public final <T> Observable<T> asRows(@NonNull final BiFunction<Cursor, Set<M>, T> mapper) {
      return Observable.create(new ObservableOnSubscribe<T>() {
        @Override public void subscribe(@NonNull ObservableEmitter<T> e) throws Throwable {
          @Nullable final Cursor cursor = run();
          if (cursor != null) {
            try {
              while (cursor.moveToNext() && !e.isDisposed()) {
                e.onNext(Objects.requireNonNull(mapper.apply(cursor, markers)));
              }
            } finally {
              cursor.close();
            }
          }
          if (!e.isDisposed()) {
            e.onComplete();
          }
        }
      });
    }
  }

  /** An executable query. */
  public static abstract class Query {
    /**
     * Creates an {@linkplain ObservableOperator operator} which transforms a query returning a
     * single row to a {@code T} using {@code mapper}. Use with {@link Observable#lift}.
     * <p>
     * It is an error for a query to pass through this operator with more than 1 row in its result
     * set. Use {@code LIMIT 1} on the underlying SQL query to prevent this. Result sets with 0 rows
     * do not emit an item.
     * <p>
     * This operator ignores {@code null} cursors returned from {@link #run()}.
     *
     * @param mapper Maps the current {@link Cursor} row to {@code T}. May not return null.
     */
    @CheckResult @NonNull //
    public static <T> ObservableOperator<T, Query> mapToOne(@NonNull Function<Cursor, T> mapper) {
      return new QueryToOneOperator<>(mapper, null);
    }

    /**
     * Creates an {@linkplain ObservableOperator operator} which transforms a query returning a
     * single row to a {@code T} using {@code mapper}. Use with {@link Observable#lift}.
     * <p>
     * It is an error for a query to pass through this operator with more than 1 row in its result
     * set. Use {@code LIMIT 1} on the underlying SQL query to prevent this. Result sets with 0 rows
     * emit {@code defaultValue}.
     * <p>
     * This operator emits {@code defaultValue} if {@code null} is returned from {@link #run()}.
     *
     * @param mapper Maps the current {@link Cursor} row to {@code T}. May not return null.
     * @param defaultValue Value returned if result set is empty
     */
    @SuppressWarnings("ConstantConditions") // Public API contract.
    @CheckResult @NonNull
    public static <T> ObservableOperator<T, Query> mapToOneOrDefault(
        @NonNull Function<Cursor, T> mapper, @NonNull T defaultValue) {
      if (defaultValue == null) throw new NullPointerException("defaultValue == null");
      return new QueryToOneOperator<>(mapper, defaultValue);
    }

    /**
     * Creates an {@linkplain ObservableOperator operator} which transforms a query returning a
     * single row to a {@code Optional<T>} using {@code mapper}. Use with {@link Observable#lift}.
     * <p>
     * It is an error for a query to pass through this operator with more than 1 row in its result
     * set. Use {@code LIMIT 1} on the underlying SQL query to prevent this. Result sets with 0 rows
     * emit {@link Optional#empty() Optional.empty()}.
     * <p>
     * This operator ignores {@code null} cursors returned from {@link #run()}.
     *
     * @param mapper Maps the current {@link Cursor} row to {@code T}. May not return null.
     */
    @RequiresApi(Build.VERSION_CODES.N) //
    @CheckResult @NonNull //
    public static <T> ObservableOperator<Optional<T>, Query> mapToOptional(
        @NonNull Function<Cursor, T> mapper) {
      return new QueryToOptionalOperator<>(mapper);
    }

    /**
     * Creates an {@linkplain ObservableOperator operator} which transforms a query to a
     * {@code List<T>} using {@code mapper}. Use with {@link Observable#lift}.
     * <p>
     * Be careful using this operator as it will always consume the entire cursor and create objects
     * for each row, every time this observable emits a new query. On tables whose queries update
     * frequently or very large result sets this can result in the creation of many objects.
     * <p>
     * This operator ignores {@code null} cursors returned from {@link #run()}.
     *
     * @param mapper Maps the current {@link Cursor} row to {@code T}. May not return null.
     */
    @CheckResult @NonNull
    public static <T> ObservableOperator<List<T>, Query> mapToList(
        @NonNull Function<Cursor, T> mapper) {
      return new QueryToListOperator<>(mapper, ArrayList::new);
    }

    /**
     * Creates an {@linkplain ObservableOperator operator} which transforms a query to a
     * {@code List<T>} using {@code mapper}. Use with {@link Observable#lift}.
     * <p>
     * Be careful using this operator as it will always consume the entire cursor and create objects
     * for each row, every time this observable emits a new query. On tables whose queries update
     * frequently or very large result sets this can result in the creation of many objects.
     * <p>
     * This operator ignores {@code null} cursors returned from {@link #run()}.
     *
     * @param mapper Maps the current {@link Cursor} row to {@code T}. May not return null.
     * @param newList Creates the new list. May not return null.
     */
    @CheckResult @NonNull
    public static <L extends List<T>, T> ObservableOperator<L, Query> mapToSpecificList(
        @NonNull Function<Cursor, T> mapper, @NonNull NewList<L, T> newList) {
      return new QueryToListOperator<>(mapper, newList);
    }

    /**
     * final because we don't want subclasses to override or else, they'll violate
     * the symmetry equals/hashCode principle.
     */
    @Override
    public final int hashCode() {
      return super.hashCode();
    }

    /**
     * final because we don't want subclasses to override or else, they'll violate
     * the symmetry equals/hashCode principle.
     */
    @Override
    public final boolean equals(@Nullable Object obj) {
      return super.equals(obj);
    }

    /**
     * Execute the query on the underlying database and return the resulting cursor.
     *
     * @return A {@link Cursor} with query results, or {@code null} when the query could not be
     * executed due to a problem with the underlying store. Unfortunately it is not well documented
     * when {@code null} is returned. It usually involves a problem in communicating with the
     * underlying store and should either be treated as failure or ignored for retry at a later
     * time.
     */
    @CheckResult @WorkerThread
    @Nullable
    public abstract Cursor run();

    /**
     * Execute the query on the underlying database and return an Observable of each row mapped to
     * {@code T} by {@code mapper}.
     * <p>
     * Standard usage of this operation is in {@code flatMap}:
     * <pre>{@code
     * flatMap(q -> q.asRows(Item.MAPPER).toList())
     * }</pre>
     * However, the above is a more-verbose but identical operation as
     * {@link QueryObservable#mapToList}. This {@code asRows} method should be used when you need
     * to limit or filter the items separate from the actual query.
     * <pre>{@code
     * flatMap(q -> q.asRows(Item.MAPPER).take(5).toList())
     * // or...
     * flatMap(q -> q.asRows(Item.MAPPER).filter(i -> i.isActive).toList())
     * }</pre>
     * <p>
     * Note: Limiting results or filtering will almost always be faster in the database as part of
     * a query and should be preferred, where possible.
     * <p>
     * The resulting observable will be empty if {@code null} is returned from {@link #run()}.
     */
    @CheckResult @NonNull
    public final <T> Observable<T> asRows(@NonNull final Function<Cursor, T> mapper) {
      return Observable.create(new ObservableOnSubscribe<T>() {
        @Override public void subscribe(@NonNull ObservableEmitter<T> e) throws Throwable {
          @Nullable final Cursor cursor = run();
          if (cursor != null) {
            try {
              while (cursor.moveToNext() && !e.isDisposed()) {
                e.onNext(Objects.requireNonNull(mapper.apply(cursor)));
              }
            } finally {
              cursor.close();
            }
          }
          if (!e.isDisposed()) {
            e.onComplete();
          }
        }
      });
    }
  }

  /** A simple indirection for logging debug messages. */
  public interface Logger {
    void log(@NonNull String message);
  }
}
