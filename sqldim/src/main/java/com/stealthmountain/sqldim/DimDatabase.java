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

import androidx.sqlite.db.SimpleSQLiteQuery;
import androidx.sqlite.db.SupportSQLiteDatabase;
import androidx.sqlite.db.SupportSQLiteOpenHelper;
import androidx.sqlite.db.SupportSQLiteOpenHelper.Callback;
import androidx.sqlite.db.SupportSQLiteQuery;
import androidx.sqlite.db.SupportSQLiteStatement;
import android.content.ContentValues;
import android.database.Cursor;
import android.database.sqlite.SQLiteTransactionListener;
import androidx.annotation.CheckResult;
import androidx.annotation.IntDef;
import androidx.annotation.NonNull;
import androidx.annotation.Nullable;
import androidx.annotation.WorkerThread;
import com.stealthmountain.sqldim.SqlDim.Logger;
import com.stealthmountain.sqldim.SqlDim.MarkedQuery;
import com.stealthmountain.sqldim.SqlDim.Query;
import io.reactivex.Observable;
import io.reactivex.ObservableTransformer;
import io.reactivex.Scheduler;
import io.reactivex.functions.Consumer;
import io.reactivex.functions.Function;
import io.reactivex.functions.Predicate;
import io.reactivex.subjects.PublishSubject;
import io.reactivex.subjects.Subject;
import java.io.Closeable;
import java.lang.annotation.Retention;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static android.database.sqlite.SQLiteDatabase.CONFLICT_ABORT;
import static android.database.sqlite.SQLiteDatabase.CONFLICT_FAIL;
import static android.database.sqlite.SQLiteDatabase.CONFLICT_IGNORE;
import static android.database.sqlite.SQLiteDatabase.CONFLICT_NONE;
import static android.database.sqlite.SQLiteDatabase.CONFLICT_REPLACE;
import static android.database.sqlite.SQLiteDatabase.CONFLICT_ROLLBACK;
import static com.stealthmountain.sqldim.MarkedQueryObservable.markedQueryObserable;
import static com.stealthmountain.sqldim.QueryObservable.QUERY_OBSERVABLE;
import static java.lang.annotation.RetentionPolicy.SOURCE;
import static java.util.Collections.singletonList;

/**
 * A lightweight wrapper around {@link SupportSQLiteOpenHelper} which allows for continuously
 * observing the result of a query. Create using a {@link SqlDim} instance.
 */
public final class DimDatabase<M> implements Closeable {
  @NonNull private final SupportSQLiteOpenHelper helper;
  @NonNull private final Logger logger;
  @NonNull private final ObservableTransformer<Query, Query> queryTransformer;
  @NonNull private final ObservableTransformer<MarkedQuery<M>, MarkedQuery<M>> markedQueryTransformer;

  // Package-private to avoid synthetic accessor method for 'transaction' instance.
  @NonNull final ThreadLocal<@org.checkerframework.checker.nullness.qual.Nullable SqliteTransaction<M>> transactions = new ThreadLocal<>();
  @NonNull private final Subject<Trigger<M>> triggers = PublishSubject.create();

  @NonNull private final Transaction<M> transaction = new Transaction<M>() {
    @Override public void markSuccessful() {
      @Nullable final SqliteTransaction<M> transaction = transactions.get();
      if (transaction == null) {
        throw new IllegalStateException("Not in transaction.");
      }
      if (logging) log("TXN SUCCESS %s", String.valueOf(transaction));
      getWritableDatabase().setTransactionSuccessful();
    }

    @Override public void markSuccessful(@NonNull M marker) {
      @Nullable final SqliteTransaction<M> transaction = transactions.get();
      if (transaction == null) {
        throw new IllegalStateException("Not in transaction.");
      }
      if (logging) log("TXN SUCCESS %s, %s", String.valueOf(marker), String.valueOf(transaction));
      transaction.markers.add(marker);
      getWritableDatabase().setTransactionSuccessful();
    }

    @Override public boolean yieldIfContendedSafely() {
      return getWritableDatabase().yieldIfContendedSafely();
    }

    @Override public boolean yieldIfContendedSafely(long sleepAmount, @NonNull TimeUnit sleepUnit) {
      return getWritableDatabase().yieldIfContendedSafely(sleepUnit.toMillis(sleepAmount));
    }

    @Override public void end() {
      @Nullable final SqliteTransaction<M> transaction = transactions.get();
      if (transaction == null) {
        throw new IllegalStateException("Not in transaction.");
      }
      @Nullable final SqliteTransaction<M> newTransaction = transaction.parent;
      transactions.set(newTransaction);
      if (logging) log("TXN END %s", transaction);
      getWritableDatabase().endTransaction();
      // Send the triggers after ending the transaction in the DB.
      if (transaction.commit) {
        // If the transaction didn't originated directly on a SupportSQLiteDatabase,
        // then we won't have a SqliteTransaction#marker.
        sendTableTrigger(transaction.markers, transaction);
      }
    }

    @Override public void close() {
      end();
    }
  };
  @NonNull private final Consumer<Object> ensureNotInTransaction = new Consumer<Object>() {
    @Override public void accept(@NonNull Object ignored) throws Exception {
      @Nullable final SqliteTransaction transaction = transactions.get();
      if (transaction != null) {
        throw new IllegalStateException("Cannot subscribe to observable query in a transaction.");
      }
    }
  };

  @NonNull private final Scheduler scheduler;

  // Package-private to avoid synthetic accessor method for 'transaction' instance.
  volatile boolean logging;

  DimDatabase(@NonNull SupportSQLiteOpenHelper helper,
              @NonNull Logger logger,
              @NonNull Scheduler scheduler,
              @NonNull ObservableTransformer<Query, Query> queryTransformer,
              @NonNull ObservableTransformer<MarkedQuery<M>, MarkedQuery<M>> markedQueryTransformer) {
    this.helper = helper;
    this.logger = logger;
    this.scheduler = scheduler;
    this.queryTransformer = queryTransformer;
    this.markedQueryTransformer = markedQueryTransformer;
  }

  /**
   * Control whether debug logging is enabled.
   */
  public void setLoggingEnabled(boolean enabled) {
    logging = enabled;
  }

  /**
   * Create and/or open a database.  This will be the same object returned by
   * {@link SupportSQLiteOpenHelper#getWritableDatabase} unless some problem, such as a full disk,
   * requires the database to be opened read-only.  In that case, a read-only
   * database object will be returned.  If the problem is fixed, a future call
   * to {@link SupportSQLiteOpenHelper#getWritableDatabase} may succeed, in which case the read-only
   * database object will be closed and the read/write object will be returned
   * in the future.
   *
   * <p class="caution">Like {@link SupportSQLiteOpenHelper#getWritableDatabase}, this method may
   * take a long time to return, so you should not call it from the
   * application main thread, including from
   * {@link android.content.ContentProvider#onCreate ContentProvider.onCreate()}.
   *
   * @throws android.database.sqlite.SQLiteException if the database cannot be opened
   * @return a database object valid until {@link SupportSQLiteOpenHelper#getWritableDatabase}
   *     or {@link #close} is called.
   */
  @NonNull @CheckResult @WorkerThread
  public SupportSQLiteDatabase getReadableDatabase() {
    return helper.getReadableDatabase();
  }

  /**
   * Create and/or open a database that will be used for reading and writing.
   * The first time this is called, the database will be opened and
   * {@link Callback#onCreate}, {@link Callback#onUpgrade} and/or {@link Callback#onOpen} will be
   * called.
   *
   * <p>Once opened successfully, the database is cached, so you can
   * call this method every time you need to write to the database.
   * (Make sure to call {@link #close} when you no longer need the database.)
   * Errors such as bad permissions or a full disk may cause this method
   * to fail, but future attempts may succeed if the problem is fixed.</p>
   *
   * <p class="caution">Database upgrade may take a long time, you
   * should not call this method from the application main thread, including
   * from {@link android.content.ContentProvider#onCreate ContentProvider.onCreate()}.
   *
   * @throws android.database.sqlite.SQLiteException if the database cannot be opened for writing
   * @return a read/write database object valid until {@link #close} is called
   */
  @NonNull @CheckResult @WorkerThread
  public SupportSQLiteDatabase getWritableDatabase() {
    return helper.getWritableDatabase();
  }

  void sendTableTrigger(@NonNull Set<M> markers, @NonNull Set<String> tables) {
    @Nullable final SqliteTransaction<M> transaction = transactions.get();
    if (transaction != null) {
      transaction.addAll(tables);
      transaction.markers.addAll(markers);
    } else {
      if (logging) log("TRIGGER %s", tables);
      triggers.onNext(new Trigger<>(markers, tables));
    }
  }

  /**
   * Begin a transaction for this thread.
   * <p>
   * Transactions may nest. If the transaction is not in progress, then a database connection is
   * obtained and a new transaction is started. Otherwise, a nested transaction is started.
   * <p>
   * Each call to {@code newTransaction} must be matched exactly by a call to
   * {@link Transaction#end()}. To mark a transaction as successful, call
   * {@link Transaction#markSuccessful(Object)} before calling {@link Transaction#end()}. If the
   * transaction is not successful, or if any of its nested transactions were not successful, then
   * the entire transaction will be rolled back when the outermost transaction is ended.
   * <p>
   * Transactions queue up all query notifications until they have been applied.
   * <p>
   * Here is the standard idiom for transactions:
   *
   * <pre>
   * try (Transaction transaction = db.newTransaction()) {
   *   ...
   *   transaction.markSuccessful();
   * }
   * }</pre>
   *
   * Manually call {@link Transaction#end()} when try-with-resources is not available:
   * <pre>
   * Transaction transaction = db.newTransaction();
   * try {
   *   ...
   *   transaction.markSuccessful();
   * } finally {
   *   transaction.end();
   * }
   * }</pre>
   *
   *
   * @see SupportSQLiteDatabase#beginTransaction()
   */
  @CheckResult @NonNull
  public Transaction<M> newTransaction() {
    @NonNull final SqliteTransaction<M> transaction = new SqliteTransaction<>(transactions.get());
    transactions.set(transaction);
    if (logging) log("TXN BEGIN %s", transaction);
    getWritableDatabase().beginTransactionWithListener(transaction);

    return this.transaction;
  }

  /**
   * Begins a transaction in IMMEDIATE mode for this thread.
   * <p>
   * Transactions may nest. If the transaction is not in progress, then a database connection is
   * obtained and a new transaction is started. Otherwise, a nested transaction is started.
   * <p>
   * Each call to {@code newNonExclusiveTransaction} must be matched exactly by a call to
   * {@link Transaction#end()}. To mark a transaction as successful, call
   * {@link Transaction#markSuccessful(Object)} before calling {@link Transaction#end()}. If the
   * transaction is not successful, or if any of its nested transactions were not successful, then
   * the entire transaction will be rolled back when the outermost transaction is ended.
   * <p>
   * Transactions queue up all query notifications until they have been applied.
   * <p>
   * Here is the standard idiom for transactions:
   *
   * <pre>
   * try (Transaction transaction = db.newNonExclusiveTransaction()) {
   *   ...
   *   transaction.markSuccessful();
   * }
   * }</pre>
   *
   * Manually call {@link Transaction#end()} when try-with-resources is not available:
   * <pre>
   * Transaction transaction = db.newNonExclusiveTransaction();
   * try {
   *   ...
   *   transaction.markSuccessful();
   * } finally {
   *   transaction.end();
   * }
   * }</pre>
   *
   *
   * @see SupportSQLiteDatabase#beginTransactionNonExclusive()
   */
  @CheckResult @NonNull
  public Transaction<M> newNonExclusiveTransaction() {
    @NonNull final SqliteTransaction<M> transaction = new SqliteTransaction<>(transactions.get());
    transactions.set(transaction);
    if (logging) log("TXN BEGIN %s", transaction);
    getWritableDatabase().beginTransactionWithListenerNonExclusive(transaction);

    return this.transaction;
  }

  /**
   * Close the underlying {@link SupportSQLiteOpenHelper} and remove cached readable and writeable
   * databases. This does not prevent existing observables from retaining existing references as
   * well as attempting to create new ones for new subscriptions.
   */
  @Override public void close() {
    helper.close();
  }

  /**
   * Create an observable which will notify subscribers with a {@linkplain Query query} for
   * execution. Subscribers are responsible for <b>always</b> closing {@link Cursor} instance
   * returned from the {@link Query}.
   * <p>
   * Subscribers will receive an immediate notification for initial data as well as subsequent
   * notifications for when the supplied {@code table}'s data changes through the {@code insert},
   * {@code update}, and {@code delete} methods of this class. Unsubscribe when you no longer want
   * updates to a query.
   * <p>
   * Since database triggers are inherently asynchronous, items emitted from the returned
   * observable use the {@link Scheduler} supplied to {@link SqlDim#wrapDatabaseHelper}. For
   * consistency, the immediate notification sent on subscribe also uses this scheduler. As such,
   * calling {@link Observable#subscribeOn subscribeOn} on the returned observable has no effect.
   * <p>
   * Note: To skip the immediate notification and only receive subsequent notifications when data
   * has changed call {@code skip(1)} on the returned observable.
   * <p>
   * <b>Warning:</b> this method does not perform the query! Only by subscribing to the returned
   * {@link Observable} will the operation occur.
   *
   * @see SupportSQLiteDatabase#query(String, Object[])
   */
  @CheckResult @NonNull
  public QueryObservable createQuery(@NonNull final String table,
                                     @NonNull String sql, @NonNull Object... args) {
    return createQuery(new DatabaseQuery(singletonList(table), new SimpleSQLiteQuery(sql, args)));
  }

  /**
   * Create an observable which will notify subscribers with a {@linkplain Query query} for
   * execution. Subscribers are responsible for <b>always</b> closing {@link Cursor} instance
   * returned from the {@link Query}.
   * <p>
   * Subscribers will receive an immediate notification for initial data as well as subsequent
   * notifications for when the supplied {@code table}'s data changes through the {@code insert},
   * {@code update}, and {@code delete} methods of this class. Unsubscribe when you no longer want
   * updates to a query.
   * <p>
   * Since database triggers are inherently asynchronous, items emitted from the returned
   * observable use the {@link Scheduler} supplied to {@link SqlDim#wrapDatabaseHelper}. For
   * consistency, the immediate notification sent on subscribe also uses this scheduler. As such,
   * calling {@link Observable#subscribeOn subscribeOn} on the returned observable has no effect.
   * <p>
   * Note: To skip the immediate notification and only receive subsequent notifications when data
   * has changed call {@code skip(1)} on the returned observable.
   * <p>
   * <b>Warning:</b> this method does not perform the query! Only by subscribing to the returned
   * {@link Observable} will the operation occur.
   *
   * @see SupportSQLiteDatabase#query(String, Object[])
   */
  @CheckResult @NonNull
  public MarkedQueryObservable<M> createMarkedQuery(@NonNull final String table,
                                                    @NonNull String sql, @NonNull Object... args) {
    return createMarkedQuery(new ToMarkedDatabaseQuery(singletonList(table), new SimpleSQLiteQuery(sql, args)));
  }

  /**
   * See {@link #createQuery(String, String, Object...)} for usage. This overload allows for
   * monitoring multiple tables for changes.
   *
   * @see SupportSQLiteDatabase#query(String, Object[])
   */
  @CheckResult @NonNull
  public QueryObservable createQuery(@NonNull final Iterable<String> tables,
                                     @NonNull String sql, @NonNull Object... args) {
    return createQuery(new DatabaseQuery(tables, new SimpleSQLiteQuery(sql, args)));
  }

  /**
   * See {@link #createMarkedQuery(String, String, Object...)} for usage. This overload allows for
   * monitoring multiple tables for changes.
   *
   * @see SupportSQLiteDatabase#query(String, Object[])
   */
  @CheckResult @NonNull
  public MarkedQueryObservable<M> createMarkedQuery(@NonNull final Iterable<String> tables,
                                                    @NonNull String sql, @NonNull Object... args) {
    return createMarkedQuery(new ToMarkedDatabaseQuery(tables, new SimpleSQLiteQuery(sql, args)));
  }

  /**
   * Create an observable which will notify subscribers with a {@linkplain Query query} for
   * execution. Subscribers are responsible for <b>always</b> closing {@link Cursor} instance
   * returned from the {@link Query}.
   * <p>
   * Subscribers will receive an immediate notification for initial data as well as subsequent
   * notifications for when the supplied {@code table}'s data changes through the {@code insert},
   * {@code update}, and {@code delete} methods of this class. Unsubscribe when you no longer want
   * updates to a query.
   * <p>
   * Since database triggers are inherently asynchronous, items emitted from the returned
   * observable use the {@link Scheduler} supplied to {@link SqlDim#wrapDatabaseHelper}. For
   * consistency, the immediate notification sent on subscribe also uses this scheduler. As such,
   * calling {@link Observable#subscribeOn subscribeOn} on the returned observable has no effect.
   * <p>
   * Note: To skip the immediate notification and only receive subsequent notifications when data
   * has changed call {@code skip(1)} on the returned observable.
   * <p>
   * <b>Warning:</b> this method does not perform the query! Only by subscribing to the returned
   * {@link Observable} will the operation occur.
   *
   * @see SupportSQLiteDatabase#query(SupportSQLiteQuery)
   */
  @CheckResult @NonNull
  public QueryObservable createQuery(@NonNull final String table,
                                     @NonNull SupportSQLiteQuery query) {
    return createQuery(new DatabaseQuery(singletonList(table), query));
  }

  /**
   * Create an observable which will notify subscribers with a {@linkplain Query query} for
   * execution. Subscribers are responsible for <b>always</b> closing {@link Cursor} instance
   * returned from the {@link Query}.
   * <p>
   * Subscribers will receive an immediate notification for initial data as well as subsequent
   * notifications for when the supplied {@code table}'s data changes through the {@code insert},
   * {@code update}, and {@code delete} methods of this class. Unsubscribe when you no longer want
   * updates to a query.
   * <p>
   * Since database triggers are inherently asynchronous, items emitted from the returned
   * observable use the {@link Scheduler} supplied to {@link SqlDim#wrapDatabaseHelper}. For
   * consistency, the immediate notification sent on subscribe also uses this scheduler. As such,
   * calling {@link Observable#subscribeOn subscribeOn} on the returned observable has no effect.
   * <p>
   * Note: To skip the immediate notification and only receive subsequent notifications when data
   * has changed call {@code skip(1)} on the returned observable.
   * <p>
   * <b>Warning:</b> this method does not perform the query! Only by subscribing to the returned
   * {@link Observable} will the operation occur.
   *
   * @see SupportSQLiteDatabase#query(SupportSQLiteQuery)
   */
  @CheckResult @NonNull
  public MarkedQueryObservable<M> createMarkedQuery(@NonNull final String table,
                                                    @NonNull SupportSQLiteQuery query) {
    return createMarkedQuery(new ToMarkedDatabaseQuery(singletonList(table), query));
  }

  /**
   * See {@link #createQuery(String, SupportSQLiteQuery)} for usage. This overload allows for
   * monitoring multiple tables for changes.
   *
   * @see SupportSQLiteDatabase#query(SupportSQLiteQuery)
   */
  @CheckResult @NonNull
  public QueryObservable createQuery(@NonNull final Iterable<String> tables,
                                     @NonNull SupportSQLiteQuery query) {
    return createQuery(new DatabaseQuery(tables, query));
  }

  /**
   * See {@link #createMarkedQuery(String, SupportSQLiteQuery)} for usage. This overload allows for
   * monitoring multiple tables for changes.
   *
   * @see SupportSQLiteDatabase#query(SupportSQLiteQuery)
   */
  @CheckResult @NonNull
  public MarkedQueryObservable<M> createMarkedQuery(@NonNull final Iterable<String> tables,
                                                    @NonNull SupportSQLiteQuery query) {
    return createMarkedQuery(new ToMarkedDatabaseQuery(tables, query));
  }

  @CheckResult @NonNull
  private QueryObservable createQuery(@NonNull DatabaseQuery query) {
    @Nullable final SqliteTransaction transaction = transactions.get();
    if (transaction != null) {
      throw new IllegalStateException("Cannot create observable query in transaction. "
              + "Use query() for a query inside a transaction.");
    }

    return triggers //
            .filter(query) // DatabaseQuery filters triggers to on tables we care about.
            .map(query) // DatabaseQuery maps to itself to save an allocation.
            .startWith(query) //
            .observeOn(scheduler) //
            .compose(queryTransformer) // Apply the user's query transformer.
            .doOnSubscribe(ensureNotInTransaction)
            .to(QUERY_OBSERVABLE);
  }

  @CheckResult @NonNull
  private MarkedQueryObservable<M> createMarkedQuery(@NonNull ToMarkedDatabaseQuery toMarkedDatabaseQuery) {
    @Nullable final SqliteTransaction transaction = transactions.get();
    if (transaction != null) {
      throw new IllegalStateException("Cannot create observable markedQuery in transaction. "
              + "Use markedQuery() for a markedQuery inside a transaction.");
    }

    return triggers //
            .filter(toMarkedDatabaseQuery) // DatabaseQuery filters triggers to on tables we care about.
            .map(toMarkedDatabaseQuery) // DatabaseQuery maps to itself to save an allocation.
            .startWith(toMarkedDatabaseQuery.initialMarkedQuery()) //
            .observeOn(scheduler) //
            .compose(markedQueryTransformer) // Apply the user's toMarkedDatabaseQuery transformer.
            .doOnSubscribe(ensureNotInTransaction)
            .to(markedQueryObserable());
  }

  /**
   * Runs the provided SQL and returns a {@link Cursor} over the result set.
   *
   * @see SupportSQLiteDatabase#query(String, Object[])
   */
  @CheckResult @NonNull @WorkerThread
  public Cursor query(@NonNull String sql, @NonNull Object... args) {
    @NonNull final Cursor cursor = Objects.requireNonNull(getReadableDatabase().query(sql, args));
    if (logging) {
      log("QUERY\n  sql: %s\n  args: %s", indentSql(sql), Arrays.toString(args));
    }

    return cursor;
  }

  /**
   * Runs the provided {@link SupportSQLiteQuery} and returns a {@link Cursor} over the result set.
   *
   * @see SupportSQLiteDatabase#query(SupportSQLiteQuery)
   */
  @CheckResult @NonNull @WorkerThread
  public Cursor query(@NonNull SupportSQLiteQuery query) {
    @NonNull final Cursor cursor = Objects.requireNonNull(getReadableDatabase().query(query));
    if (logging) {
      log("QUERY\n  sql: %s", indentSql(query.getSql()));
    }

    return cursor;
  }

  /**
   * Insert a row into the specified {@code table} and notify any subscribed queries.
   *
   * Includes no marker
   *
   * @see SupportSQLiteDatabase#insert(String, int, ContentValues)
   */
  @WorkerThread
  public long insert(@NonNull String table, @ConflictAlgorithm int conflictAlgorithm,
                     @NonNull ContentValues values) {
    return insertMarked(Collections.emptySet(), table, conflictAlgorithm, values);
  }

  /**
   * Insert a row into the specified {@code table} and notify any subscribed queries.
   *
   * @see SupportSQLiteDatabase#insert(String, int, ContentValues)
   */
  @WorkerThread
  public long insertMarked(@NonNull M marker, @NonNull String table, @ConflictAlgorithm int conflictAlgorithm,
                           @NonNull ContentValues values) {
    return insertMarked(Collections.singleton(marker), table, conflictAlgorithm, values);
  }

  @WorkerThread
  private long insertMarked(@NonNull Set<M> markers, @NonNull String table, @ConflictAlgorithm int conflictAlgorithm,
                            @NonNull ContentValues values) {
    @NonNull final SupportSQLiteDatabase db = getWritableDatabase();

    if (logging) {
      log("INSERT\n  markers: %s\n  table: %s\n  values: %s\n  conflictAlgorithm: %s",
              markers,
              table,
              values,
              conflictString(conflictAlgorithm)
      );
    }
    final long rowId = db.insert(table, conflictAlgorithm, values);

    if (logging) log("INSERT id: %s", rowId);

    if (rowId != -1) {
      // Only send a table trigger if the insert was successful.
      sendTableTrigger(markers, Collections.singleton(table));
    }
    return rowId;
  }

  /**
   * Delete rows from the specified {@code table} and notify any subscribed queries. This method
   * will not trigger a notification if no rows were deleted.
   *
   * Includes no marker
   *
   * @see SupportSQLiteDatabase#delete(String, String, Object[])
   */
  // inlined because we can't declare: @Nullable String @Nullable ... whereArgs
  // https://github.com/typetools/checker-framework/issues/2923
  @WorkerThread
  public int delete(@NonNull String table, @Nullable String whereClause,
                    @Nullable String... whereArgs) {
    Set<M> markers = Collections.emptySet();
    @NonNull final SupportSQLiteDatabase db = getWritableDatabase();

    if (logging) {
      log("DELETE\n  markers: %s\n  table: %s\n  whereClause: %s\n  whereArgs: %s",
              markers,
              table,
              String.valueOf(whereClause),
              Arrays.toString(whereArgs));
    }
    final int rows = db.delete(table, whereClause, whereArgs);

    if (logging) log("DELETE affected %s %s", rows, rows != 1 ? "rows" : "row");

    if (rows > 0) {
      // Only send a table trigger if rows were affected.
      sendTableTrigger(markers, Collections.singleton(table));
    }
    return rows;
  }

  /**
   * Delete rows from the specified {@code table} and notify any subscribed queries. This method
   * will not trigger a notification if no rows were deleted.
   *
   * @see SupportSQLiteDatabase#delete(String, String, Object[])
   */
  // inlined because we can't declare: @Nullable String @Nullable ... whereArgs
  // https://github.com/typetools/checker-framework/issues/2923
  @WorkerThread
  public int deleteMarked(@NonNull M marker, @NonNull String table, @Nullable String whereClause,
                          @Nullable String... whereArgs) {
    Set<M> markers = Collections.singleton(marker);
    @NonNull final SupportSQLiteDatabase db = getWritableDatabase();

    if (logging) {
      log("DELETE\n  markers: %s\n  table: %s\n  whereClause: %s\n  whereArgs: %s",
              markers,
              table,
              String.valueOf(whereClause),
              Arrays.toString(whereArgs));
    }
    final int rows = db.delete(table, whereClause, whereArgs);

    if (logging) log("DELETE affected %s %s", rows, rows != 1 ? "rows" : "row");

    if (rows > 0) {
      // Only send a table trigger if rows were affected.
      sendTableTrigger(markers, Collections.singleton(table));
    }
    return rows;
  }

  /**
   * Update rows in the specified {@code table} and notify any subscribed queries. This method
   * will not trigger a notification if no rows were updated.
   *
   * Includes no marker
   *
   * @see SupportSQLiteDatabase#update(String, int, ContentValues, String, Object[])
   */
  // inlined because we can't declare: @Nullable String @Nullable ... whereArgs
  // https://github.com/typetools/checker-framework/issues/2923
  @WorkerThread
  public int update(@NonNull String table, @ConflictAlgorithm int conflictAlgorithm,
                    @NonNull ContentValues values, @Nullable String whereClause, @Nullable String... whereArgs) {
    Set<M> markers = Collections.emptySet();
    @NonNull final SupportSQLiteDatabase db = getWritableDatabase();

    if (logging) {
      log("UPDATE\n  markers: %s\n  table: %s\n  values: %s\n  whereClause: %s\n  whereArgs: %s\n  conflictAlgorithm: %s",
              markers,
              table,
              values,
              String.valueOf(whereClause),
              Arrays.toString(whereArgs),
              conflictString(conflictAlgorithm)
      );
    }
    final int rows = db.update(table, conflictAlgorithm, values, whereClause, whereArgs);

    if (logging) log("UPDATE affected %s %s", rows, rows != 1 ? "rows" : "row");

    if (rows > 0) {
      // Only send a table trigger if rows were affected.
      sendTableTrigger(markers, Collections.singleton(table));
    }
    return rows;
  }

  /**
   * Update rows in the specified {@code table} and notify any subscribed queries. This method
   * will not trigger a notification if no rows were updated.
   *
   * @see SupportSQLiteDatabase#update(String, int, ContentValues, String, Object[])
   */
  // inlined because we can't declare: @Nullable String @Nullable ... whereArgs
  // https://github.com/typetools/checker-framework/issues/2923
  @WorkerThread
  public int updateMarked(@NonNull M marker, @NonNull String table, @ConflictAlgorithm int conflictAlgorithm,
                          @NonNull ContentValues values, @Nullable String whereClause, @Nullable String... whereArgs) {
    Set<M> markers = Collections.singleton(marker);
    @NonNull final SupportSQLiteDatabase db = getWritableDatabase();

    if (logging) {
      log("UPDATE\n  markers: %s\n  table: %s\n  values: %s\n  whereClause: %s\n  whereArgs: %s\n  conflictAlgorithm: %s",
              markers,
              table,
              values,
              String.valueOf(whereClause),
              Arrays.toString(whereArgs),
              conflictString(conflictAlgorithm)
      );
    }
    final int rows = db.update(table, conflictAlgorithm, values, whereClause, whereArgs);

    if (logging) log("UPDATE affected %s %s", rows, rows != 1 ? "rows" : "row");

    if (rows > 0) {
      // Only send a table trigger if rows were affected.
      sendTableTrigger(markers, Collections.singleton(table));
    }
    return rows;
  }

  /**
   * Execute {@code sql} provided it is NOT a {@code SELECT} or any other SQL statement that
   * returns data. No data can be returned (such as the number of affected rows). Instead, use
   * {@link #insert}, {@link #update}, et al, when possible.
   * <p>
   * No notifications will be sent to queries if {@code sql} affects the data of a table.
   *
   * @see SupportSQLiteDatabase#execSQL(String)
   */
  @WorkerThread
  public void execute(@NonNull String sql) {
    if (logging) log("EXECUTE\n  sql: %s", indentSql(sql));

    // SupportSQLiteDatabase#execSQL(String) isn't annotated, but sql gets passed
    // through to android.database.DatabaseUtils#getSqlStatementType(String), which
    // is also not annotated, but does require sql to be @NonNull.
    getWritableDatabase().execSQL(sql);
  }

  /**
   * Execute {@code sql} provided it is NOT a {@code SELECT} or any other SQL statement that
   * returns data. No data can be returned (such as the number of affected rows). Instead, use
   * {@link #insert}, {@link #update}, et al, when possible.
   * <p>
   * No notifications will be sent to queries if {@code sql} affects the data of a table.
   *
   * @see SupportSQLiteDatabase#execSQL(String, Object[])
   */
  @WorkerThread
  public void execute(@NonNull String sql, @NonNull Object... args) {
    if (logging) log("EXECUTE\n  sql: %s\n  args: %s", indentSql(sql), Arrays.toString(args));

    // SupportSQLiteDatabase#execSQL(String) isn't annotated, but sql gets passed
    // through to android.database.DatabaseUtils#getSqlStatementType(String), which
    // is also not annotated, but does require sql to be @NonNull.
    // android.database.sqlite.SQLiteDatabase#execSQL(String,Object[]) explicitly
    // checks for a @NonNull args, but the method itself isn't annotated.
    getWritableDatabase().execSQL(sql, args);
  }

  /**
   * Execute {@code sql} provided it is NOT a {@code SELECT} or any other SQL statement that
   * returns data. No data can be returned (such as the number of affected rows). Instead, use
   * {@link #insert}, {@link #update}, et al, when possible.
   * <p>
   * A notification to queries for {@code table} will be sent after the statement is executed.
   *
   * Includes no marker
   *
   * @see SupportSQLiteDatabase#execSQL(String)
   */
  @WorkerThread
  public void executeAndTrigger(@NonNull String table, @NonNull String sql) {
    executeAndTriggerMarked(Collections.emptySet(), Collections.singleton(table), sql);
  }

  /**
   * Execute {@code sql} provided it is NOT a {@code SELECT} or any other SQL statement that
   * returns data. No data can be returned (such as the number of affected rows). Instead, use
   * {@link #insert}, {@link #update}, et al, when possible.
   * <p>
   * A notification to queries for {@code table} will be sent after the statement is executed.
   *
   * @see SupportSQLiteDatabase#execSQL(String)
   */
  @WorkerThread
  public void executeAndTriggerMarked(@NonNull M marker, @NonNull String table, @NonNull String sql) {
    executeAndTriggerMarked(marker, Collections.singleton(table), sql);
  }

  /**
   * See {@link #executeAndTriggerMarked(Object, String, String)} for usage. This overload allows for triggering multiple tables.
   *
   * Includes no marker
   *
   * @see DimDatabase#executeAndTriggerMarked(Object, String, String)
   */
  @WorkerThread
  public void executeAndTrigger(@NonNull Set<String> tables, @NonNull String sql) {
    executeAndTriggerMarked(Collections.emptySet(), tables, sql);
  }

  /**
   * See {@link #executeAndTriggerMarked(Object, String, String)} for usage. This overload allows for triggering multiple tables.
   *
   * @see DimDatabase#executeAndTriggerMarked(Object, String, String)
   */
  @WorkerThread
  public void executeAndTriggerMarked(@NonNull M marker, @NonNull Set<String> tables, @NonNull String sql) {
    execute(sql);

    sendTableTrigger(Collections.singleton(marker), tables);
  }

  /**
   * Execute {@code sql} provided it is NOT a {@code SELECT} or any other SQL statement that
   * returns data. No data can be returned (such as the number of affected rows). Instead, use
   * {@link #insert}, {@link #update}, et al, when possible.
   * <p>
   * A notification to queries for {@code table} will be sent after the statement is executed.
   *
   * Includes no marker
   *
   * @see SupportSQLiteDatabase#execSQL(String, Object[])
   */
  @WorkerThread
  public void executeAndTrigger(@NonNull String table, @NonNull String sql, @NonNull Object... args) {
    executeAndTriggerMarked(Collections.emptySet(), Collections.singleton(table), sql, args);
  }

  /**
   * Execute {@code sql} provided it is NOT a {@code SELECT} or any other SQL statement that
   * returns data. No data can be returned (such as the number of affected rows). Instead, use
   * {@link #insert}, {@link #update}, et al, when possible.
   * <p>
   * A notification to queries for {@code table} will be sent after the statement is executed.
   *
   * @see SupportSQLiteDatabase#execSQL(String, Object[])
   */
  @WorkerThread
  public void executeAndTriggerMarked(@NonNull M marker, @NonNull String table,
                                      @NonNull String sql, @NonNull Object... args) {
    executeAndTriggerMarked(Collections.singleton(marker), Collections.singleton(table), sql, args);
  }

  /**
   * See {@link #executeAndTriggerMarked(Object, String, String, Object...)} for usage. This overload allows for triggering multiple tables.
   *
   * Includes no marker
   *
   * @see DimDatabase#executeAndTriggerMarked(Object, String, String, Object...)
   */
  @WorkerThread
  public void executeAndTrigger(@NonNull Set<String> tables, @NonNull String sql, @NonNull Object... args) {
    executeAndTriggerMarked(Collections.emptySet(), tables, sql, args);
  }

  /**
   * See {@link #executeAndTriggerMarked(Object, String, String, Object...)} for usage. This overload allows for triggering multiple tables.
   *
   * @see DimDatabase#executeAndTriggerMarked(Object, String, String, Object...)
   */
  @WorkerThread
  public void executeAndTriggerMarked(@NonNull M marker, @NonNull Set<String> tables,
                                      @NonNull String sql, @NonNull Object... args) {
    execute(sql, args);

    sendTableTrigger(Collections.singleton(marker), tables);
  }

  @WorkerThread
  private void executeAndTriggerMarked(@NonNull Set<M> markers, @NonNull Set<String> tables,
                                       @NonNull String sql, @NonNull Object... args) {
    execute(sql, args);

    sendTableTrigger(markers, tables);
  }

  /**
   * Execute {@code statement}, if the the number of rows affected by execution of this SQL
   * statement is of any importance to the caller - for example, UPDATE / DELETE SQL statements.
   *
   * Includes no marker
   *
   * @return the number of rows affected by this SQL statement execution.
   * @throws android.database.SQLException If the SQL string is invalid
   *
   * @see SupportSQLiteStatement#executeUpdateDelete()
   */
  @WorkerThread
  public int executeUpdateDelete(@NonNull String table,
                                 @NonNull SupportSQLiteStatement statement) {
    return executeUpdateDeleteMarked(Collections.emptySet(), Collections.singleton(table), statement);
  }

  /**
   * Execute {@code statement}, if the the number of rows affected by execution of this SQL
   * statement is of any importance to the caller - for example, UPDATE / DELETE SQL statements.
   *
   * @return the number of rows affected by this SQL statement execution.
   * @throws android.database.SQLException If the SQL string is invalid
   *
   * @see SupportSQLiteStatement#executeUpdateDelete()
   */
  @WorkerThread
  public int executeUpdateDeleteMarked(@NonNull M marker, @NonNull String table,
                                       @NonNull SupportSQLiteStatement statement) {
    return executeUpdateDeleteMarked(Collections.singleton(marker), Collections.singleton(table), statement);
  }

  /**
   * See {@link #executeUpdateDeleteMarked(Object, String, SupportSQLiteStatement)} for usage. This overload
   * allows for triggering multiple tables.
   *
   * Includes no marker
   *
   * @see DimDatabase#executeUpdateDeleteMarked(Object, String, SupportSQLiteStatement)
   */
  @WorkerThread
  public int executeUpdateDelete(@NonNull Set<String> tables, @NonNull SupportSQLiteStatement statement) {
    return executeUpdateDeleteMarked(Collections.emptySet(), tables, statement);
  }

  /**
   * See {@link #executeUpdateDeleteMarked(Object, String, SupportSQLiteStatement)} for usage. This overload
   * allows for triggering multiple tables.
   *
   * @see DimDatabase#executeUpdateDeleteMarked(Object, String, SupportSQLiteStatement)
   */
  @WorkerThread
  public int executeUpdateDeleteMarked(@NonNull M marker, @NonNull Set<String> tables,
                                       @NonNull SupportSQLiteStatement statement) {
    return executeUpdateDeleteMarked(Collections.singleton(marker), tables, statement);
  }

  @WorkerThread
  private int executeUpdateDeleteMarked(@NonNull Set<M> markers, @NonNull Set<String> tables,
                                        @NonNull SupportSQLiteStatement statement) {
    if (logging) log("EXECUTE\n %s", statement);

    final int rows = statement.executeUpdateDelete();
    if (rows > 0) {
      // Only send a table trigger if rows were affected.
      sendTableTrigger(markers, tables);
    }
    return rows;
  }

  /**
   * Execute {@code statement} and return the ID of the row inserted due to this call.
   * The SQL statement should be an INSERT for this to be a useful call.
   *
   * Includes no marker
   *
   * @return the row ID of the last row inserted, if this insert is successful. -1 otherwise.
   *
   * @throws android.database.SQLException If the SQL string is invalid
   *
   * @see SupportSQLiteStatement#executeInsert()
   */
  @WorkerThread
  public long executeInsert(@NonNull String table, @NonNull SupportSQLiteStatement statement) {
    return executeInsertMarked(Collections.emptySet(), Collections.singleton(table), statement);
  }

  /**
   * Execute {@code statement} and return the ID of the row inserted due to this call.
   * The SQL statement should be an INSERT for this to be a useful call.
   *
   * @return the row ID of the last row inserted, if this insert is successful. -1 otherwise.
   *
   * @throws android.database.SQLException If the SQL string is invalid
   *
   * @see SupportSQLiteStatement#executeInsert()
   */
  @WorkerThread
  public long executeInsertMarked(@NonNull M marker, @NonNull String table,
                                  @NonNull SupportSQLiteStatement statement) {
    return executeInsertMarked(Collections.singleton(marker), Collections.singleton(table), statement);
  }

  /**
   * See {@link #executeInsertMarked(Object, String, SupportSQLiteStatement)} for usage. This overload allows for
   * triggering multiple tables.
   *
   * Includes no marker
   *
   * @see DimDatabase#executeInsertMarked(Object, String, SupportSQLiteStatement)
   */
  @WorkerThread
  public long executeInsert(@NonNull Set<String> tables, @NonNull SupportSQLiteStatement statement) {
    return executeInsertMarked(Collections.emptySet(), tables, statement);
  }

  /**
   * See {@link #executeInsertMarked(Object, String, SupportSQLiteStatement)} for usage. This overload allows for
   * triggering multiple tables.
   *
   * @see DimDatabase#executeInsertMarked(Object, String, SupportSQLiteStatement)
   */
  @WorkerThread
  public long executeInsertMarked(@NonNull M marker, @NonNull Set<String> tables,
                                  @NonNull SupportSQLiteStatement statement) {
    return executeInsertMarked(Collections.singleton(marker), tables, statement);
  }

  @WorkerThread
  private long executeInsertMarked(@NonNull Set<M> markers, @NonNull Set<String> tables,
                                   @NonNull SupportSQLiteStatement statement) {
    if (logging) log("EXECUTE\n %s", statement);

    final long rowId = statement.executeInsert();
    if (rowId != -1) {
      // Only send a table trigger if the insert was successful.
      sendTableTrigger(markers, tables);
    }
    return rowId;
  }

  /** An in-progress database transaction. */
  public interface Transaction<M> extends Closeable {
    /**
     * End a transaction. See {@link #newTransaction()} for notes about how to use this and when
     * transactions are committed and rolled back.
     *
     * @see SupportSQLiteDatabase#endTransaction()
     */
    @WorkerThread
    void end();

    /**
     * Marks the current transaction as successful. Do not do any more database work between
     * calling this and calling {@link #end()}. Do as little non-database work as possible in that
     * situation too. If any errors are encountered between this and {@link #end()} the transaction
     * will still be committed.
     *
     * Includes no marker
     *
     * @see SupportSQLiteDatabase#setTransactionSuccessful()
     */
    @WorkerThread
    void markSuccessful();

    /**
     * Marks the current transaction as successful. Do not do any more database work between
     * calling this and calling {@link #end()}. Do as little non-database work as possible in that
     * situation too. If any errors are encountered between this and {@link #end()} the transaction
     * will still be committed.
     *
     * @param marker A value that will be sent along with active queries to mark the results
     *               as coming from this transaction.
     *
     * @see SupportSQLiteDatabase#setTransactionSuccessful()
     */
    @WorkerThread
    void markSuccessful(@NonNull M marker);

    /**
     * Temporarily end the transaction to let other threads run. The transaction is assumed to be
     * successful so far. Do not call {@link #markSuccessful(Object)} before calling this. When this
     * returns a new transaction will have been created but not marked as successful. This assumes
     * that there are no nested transactions (newTransaction has only been called once) and will
     * throw an exception if that is not the case.
     *
     * @return true if the transaction was yielded
     *
     * @see SupportSQLiteDatabase#yieldIfContendedSafely()
     */
    @WorkerThread
    boolean yieldIfContendedSafely();

    /**
     * Temporarily end the transaction to let other threads run. The transaction is assumed to be
     * successful so far. Do not call {@link #markSuccessful(Object)} before calling this. When this
     * returns a new transaction will have been created but not marked as successful. This assumes
     * that there are no nested transactions (newTransaction has only been called once) and will
     * throw an exception if that is not the case.
     *
     * @param sleepAmount if > 0, sleep this long before starting a new transaction if
     *   the lock was actually yielded. This will allow other background threads to make some
     *   more progress than they would if we started the transaction immediately.
     * @return true if the transaction was yielded
     *
     * @see SupportSQLiteDatabase#yieldIfContendedSafely(long)
     */
    @WorkerThread
    boolean yieldIfContendedSafely(long sleepAmount, @NonNull TimeUnit sleepUnit);

    /**
     * Equivalent to calling {@link #end()}
     */
    @WorkerThread
    @Override void close();
  }

  @IntDef({
      CONFLICT_ABORT,
      CONFLICT_FAIL,
      CONFLICT_IGNORE,
      CONFLICT_NONE,
      CONFLICT_REPLACE,
      CONFLICT_ROLLBACK
  })
  @Retention(SOURCE)
  private @interface ConflictAlgorithm {
  }

  // Package-private to avoid synthetic accessor method for 'DatabaseQuery' instances.
  @NonNull
  static String indentSql(@NonNull String sql) {
    return sql.replace("\n", "\n       ");
  }

  // Package-private to avoid synthetic accessor method for 'transaction' instance.
  void log(@NonNull String message, @NonNull Object... args) {
    if (args.length > 0) message = String.format(message, args);
    logger.log(message);
  }

  private static String conflictString(@ConflictAlgorithm int conflictAlgorithm) {
    switch (conflictAlgorithm) {
      case CONFLICT_ABORT:
        return "abort";
      case CONFLICT_FAIL:
        return "fail";
      case CONFLICT_IGNORE:
        return "ignore";
      case CONFLICT_NONE:
        return "none";
      case CONFLICT_REPLACE:
        return "replace";
      case CONFLICT_ROLLBACK:
        return "rollback";
      default:
        return "unknown (" + conflictAlgorithm + ')';
    }
  }

  static final class Trigger<M> {
    @NonNull final Set<M> markers;
    @NonNull final Set<String> tables;

    Trigger(@NonNull Set<M> markers, @NonNull Set<String> tables) {
      this.tables = tables;
      this.markers = markers;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      Trigger<?> trigger = (Trigger<?>) o;

      if (!markers.equals(trigger.markers)) return false;
      return tables.equals(trigger.tables);
    }

    @Override
    public int hashCode() {
      int result = markers.hashCode();
      result = 31 * result + tables.hashCode();
      return result;
    }

    @Override
    public String toString() {
      return "Trigger[" +
              "markers=" + markers +
              ", tables=" + tables +
              ']';
    }
  }

  static final class SqliteTransaction<M> extends LinkedHashSet<String>
      implements SQLiteTransactionListener {
    @Nullable final SqliteTransaction<M> parent;
    boolean commit;
    @NonNull final HashSet<M> markers = new HashSet<>();

    SqliteTransaction(@Nullable SqliteTransaction<M> parent) {
      this.parent = parent;
    }

    @Override public void onBegin() {
    }

    @Override public void onCommit() {
      commit = true;
    }

    @Override public void onRollback() {
    }

    @NonNull @Override public String toString() {
      @NonNull final String name = String.format("%08x", System.identityHashCode(this));
      return parent == null ? name : name + " [" + parent.toString() + ']';
    }
  }

  final class ToMarkedDatabaseQuery
          implements Function<Trigger<M>, MarkedQuery<M>>, Predicate<Trigger<M>> {
    @NonNull private final Iterable<String> tables;
    @NonNull private final SupportSQLiteQuery query;

    ToMarkedDatabaseQuery(@NonNull Iterable<String> tables,
                          @NonNull SupportSQLiteQuery query) {
      this.tables = tables;
      this.query = query;
    }

    @NonNull MarkedQuery<M> initialMarkedQuery() {
      return new MarkedDatabaseQuery(Collections.emptySet(), tables, query);
    }

    @NonNull @Override public MarkedQuery<M> apply(@NonNull Trigger<M> trigger) {
      return new MarkedDatabaseQuery(trigger.markers, tables, query);
    }

    @Override public boolean test(@NonNull Trigger<M> trigger) {
      for (String table : tables) {
        if (trigger.tables.contains(table)) {
          return true;
        }
      }
      return false;
    }
  }

  final class MarkedDatabaseQuery extends MarkedQuery<M> {
    @NonNull private final Iterable<String> tables;
    @NonNull private final SupportSQLiteQuery query;

    MarkedDatabaseQuery(@NonNull Set<M> markers,
                        @NonNull Iterable<String> tables,
                        @NonNull SupportSQLiteQuery query) {
      super(markers);
      this.tables = tables;
      this.query = query;
    }

    @Nullable @Override public Cursor run() {
      if (transactions.get() != null) {
        throw new IllegalStateException("Cannot execute observable query in a transaction.");
      }

      @Nullable final Cursor cursor = getReadableDatabase().query(query);

      if (logging) {
        log("QUERY\n  markers: %s\n  tables: %s\n  sql: %s",
                markers,
                tables,
                indentSql(query.getSql())
        );
      }

      return cursor;
    }

    @NonNull @Override public String toString() {
      return query.getSql();
    }
  }

  final class DatabaseQuery extends Query
      implements Function<Trigger<M>, Query>, Predicate<Trigger<M>> {
    @NonNull private final Iterable<String> tables;
    @NonNull private final SupportSQLiteQuery query;

    DatabaseQuery(@NonNull Iterable<String> tables,
                  @NonNull SupportSQLiteQuery query) {
      this.tables = tables;
      this.query = query;
    }

    @Nullable @Override public Cursor run() {
      if (transactions.get() != null) {
        throw new IllegalStateException("Cannot execute observable query in a transaction.");
      }

      @Nullable final Cursor cursor = getReadableDatabase().query(query);

      if (logging) {
        log("QUERY\n  tables: %s\n  sql: %s", tables, indentSql(query.getSql()));
      }

      return cursor;
    }

    @NonNull @Override public String toString() {
      return query.getSql();
    }

    @NonNull @Override public Query apply(@NonNull Trigger<M> ignored) {
      return this;
    }

    @Override public boolean test(@NonNull Trigger<M> trigger) {
      for (String table : tables) {
        if (trigger.tables.contains(table)) {
          return true;
        }
      }
      return false;
    }
  }
}
