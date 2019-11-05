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
package com.squareup.sqlbrite3;

import android.annotation.TargetApi;
import android.content.ContentValues;
import android.database.Cursor;
import android.database.SQLException;
import android.database.sqlite.SQLiteException;
import android.os.Build;

import androidx.annotation.NonNull;
import androidx.annotation.Nullable;
import androidx.sqlite.db.SimpleSQLiteQuery;
import androidx.sqlite.db.SupportSQLiteDatabase;
import androidx.sqlite.db.SupportSQLiteOpenHelper;
import androidx.sqlite.db.SupportSQLiteOpenHelper.Configuration;
import androidx.sqlite.db.SupportSQLiteOpenHelper.Factory;
import androidx.sqlite.db.SupportSQLiteStatement;
import androidx.sqlite.db.framework.FrameworkSQLiteOpenHelperFactory;
import androidx.test.filters.SdkSuppress;
import androidx.test.platform.app.InstrumentationRegistry;

import com.squareup.sqlbrite3.BriteDatabase.Transaction;
import com.squareup.sqlbrite3.RecordingObserver.CursorAssert;
import com.squareup.sqlbrite3.TestDb.Employee;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import javax.annotation.Nonnull;

import io.reactivex.Observable;
import io.reactivex.ObservableSource;
import io.reactivex.ObservableTransformer;
import io.reactivex.functions.Consumer;
import io.reactivex.observers.TestObserver;
import io.reactivex.subjects.PublishSubject;

import static android.database.sqlite.SQLiteDatabase.CONFLICT_IGNORE;
import static android.database.sqlite.SQLiteDatabase.CONFLICT_NONE;
import static com.google.common.truth.Truth.assertThat;
import static com.squareup.sqlbrite3.SqlBrite.MarkedQuery;
import static com.squareup.sqlbrite3.SqlBrite.MarkedQuery.MarkedValue;
import static com.squareup.sqlbrite3.SqlBrite.Query;
import static com.squareup.sqlbrite3.TestDb.BOTH_TABLES;
import static com.squareup.sqlbrite3.TestDb.EmployeeTable.NAME;
import static com.squareup.sqlbrite3.TestDb.EmployeeTable.USERNAME;
import static com.squareup.sqlbrite3.TestDb.SELECT_EMPLOYEES;
import static com.squareup.sqlbrite3.TestDb.SELECT_MANAGER_LIST;
import static com.squareup.sqlbrite3.TestDb.TABLE_EMPLOYEE;
import static com.squareup.sqlbrite3.TestDb.TABLE_MANAGER;
import static com.squareup.sqlbrite3.TestDb.employee;
import static com.squareup.sqlbrite3.TestDb.manager;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.fail;

public final class MarkedBriteDatabaseTest {
  @NonNull  private final TestDb testDb = new TestDb();
  @NonNull  private final List<String> logs = new ArrayList<>();
  @NonNull  private final MarkedRecordingObserver o = new MarkedRecordingObserver();
  @NonNull  private final TestScheduler scheduler = new TestScheduler();
  @NonNull  private final PublishSubject<Object> killSwitch = PublishSubject.create();

  @NonNull @Rule public final TemporaryFolder dbFolder = new TemporaryFolder();

  @Nullable private SupportSQLiteDatabase real;
  @Nullable private BriteDatabase<String> db;

  @Before public void setUp() throws IOException {
    @NonNull final Configuration configuration = Configuration.builder(
            InstrumentationRegistry.getInstrumentation().getTargetContext()
    )
        .callback(testDb)
        .name(dbFolder.newFile().getPath())
        .build();

    @NonNull final Factory factory = new FrameworkSQLiteOpenHelperFactory();
    @NonNull final SupportSQLiteOpenHelper helper = factory.create(configuration);
    real = helper.getWritableDatabase();

    @NonNull final SqlBrite.Logger logger = new SqlBrite.Logger() {
      @Override public void log(@NonNull String message) {
        logs.add(message);
      }
    };
    @NonNull final ObservableTransformer<Query, Query> queryTransformer =
        new ObservableTransformer<Query, Query>() {
          @Override public ObservableSource<Query> apply(Observable<Query> upstream) {
            return upstream.takeUntil(killSwitch);
          }
        };
    @NonNull final ObservableTransformer<MarkedQuery<String>, MarkedQuery<String>> markedQueryTransformer =
        new ObservableTransformer<MarkedQuery<String>, MarkedQuery<String>>() {
          @Override
          public ObservableSource<MarkedQuery<String>> apply(Observable<MarkedQuery<String>> upstream) {
            return upstream.takeUntil(killSwitch);
          }
        };
    db = new BriteDatabase<>(helper, logger, scheduler, queryTransformer, markedQueryTransformer);
  }

  @After public void tearDown() {
    o.assertNoMoreEvents();
  }

  @Test public void loggerEnabled() {
    @NonNull final BriteDatabase<String> db = Objects.requireNonNull(this.db);

    db.setLoggingEnabled(true);
    db.insertMarked("marker", TABLE_EMPLOYEE, CONFLICT_NONE, employee("john", "John Johnson"));
    assertThat(logs).isNotEmpty();
  }

  @Test public void loggerDisabled() {
    @NonNull final BriteDatabase<String> db = Objects.requireNonNull(this.db);

    db.setLoggingEnabled(false);
    db.insertMarked("marker", TABLE_EMPLOYEE, CONFLICT_NONE, employee("john", "John Johnson"));
    assertThat(logs).isEmpty();
  }

  @Test public void loggerIndentsSqlForCreateMarkedQuery() {
    @NonNull final BriteDatabase<String> db = Objects.requireNonNull(this.db);

    db.setLoggingEnabled(true);
    @NonNull final MarkedQueryObservable<String> query = db.createMarkedQuery(TABLE_EMPLOYEE, "SELECT\n1");
    query.subscribe(new Consumer<MarkedQuery<String>>() {
      @Override public void accept(@NonNull MarkedQuery<String> markedQuery) throws Exception {
        Objects.requireNonNull(markedQuery.run()).close();
      }
    }).isDisposed();
    assertThat(logs).containsExactly(""
        + "QUERY\n"
        + "  markers: []\n"
        + "  tables: [employee]\n"
        + "  sql: SELECT\n"
        + "       1");
  }

  @Test public void loggerIndentsSqlForQuery() {
    @NonNull final BriteDatabase<String> db = Objects.requireNonNull(this.db);

    db.setLoggingEnabled(true);
    db.query("SELECT\n1").close();
    assertThat(logs).containsExactly(""
        + "QUERY\n"
        + "  sql: SELECT\n"
        + "       1\n"
        + "  args: []");
  }

  @Test public void loggerIndentsSqlForExecute() {
    @NonNull final BriteDatabase<String> db = Objects.requireNonNull(this.db);

    db.setLoggingEnabled(true);
    db.execute("PRAGMA\ncompile_options");
    assertThat(logs).containsExactly(""
        + "EXECUTE\n"
        + "  sql: PRAGMA\n"
        + "       compile_options");
  }

  @Test public void loggerIndentsSqlForExecuteWithArgs() {
    @NonNull final BriteDatabase<String> db = Objects.requireNonNull(this.db);

    db.setLoggingEnabled(true);
    db.execute("PRAGMA\ncompile_options", new Object[0]);
    assertThat(logs).containsExactly(""
        + "EXECUTE\n"
        + "  sql: PRAGMA\n"
        + "       compile_options\n"
        + "  args: []");
  }

  @Test public void closePropagates() {
    @NonNull final BriteDatabase<String> db = Objects.requireNonNull(this.db);
    @NonNull final SupportSQLiteDatabase real = Objects.requireNonNull(this.real);

    db.close();
    assertThat(real.isOpen()).isFalse();
  }

  @Test public void markedQuery() {
    @NonNull final BriteDatabase<String> db = Objects.requireNonNull(this.db);

    db.createMarkedQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES).subscribe(o);
    o.assertEmptyMarkers();
    o.assertCursor()
        .hasRow("alice", "Alice Allison")
        .hasRow("bob", "Bob Bobberson")
        .hasRow("eve", "Eve Evenson")
        .isExhausted();
  }

  @Test public void markedQueryWithQueryObject() {
    @NonNull final BriteDatabase<String> db = Objects.requireNonNull(this.db);

    db.createMarkedQuery(TABLE_EMPLOYEE, new SimpleSQLiteQuery(SELECT_EMPLOYEES)).subscribe(o);
    o.assertEmptyMarkers();
    o.assertCursor()
        .hasRow("alice", "Alice Allison")
        .hasRow("bob", "Bob Bobberson")
        .hasRow("eve", "Eve Evenson")
        .isExhausted();
  }

  @Test public void emptyMarkedQueryMapToList() {
    @NonNull final BriteDatabase<String> db = Objects.requireNonNull(this.db);

    @NonNull final MarkedValue<String, List<Employee>> employeesMarkedValue =
            db.createMarkedQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES)
                    .mapToList(Employee.MARKED_MAPPER)
                    .blockingFirst();
    assertThat(employeesMarkedValue.markers).isEmpty();
    assertThat(employeesMarkedValue.value).containsExactly( //
            new Employee("alice", "Alice Allison"), //
            new Employee("bob", "Bob Bobberson"), //
            new Employee("eve", "Eve Evenson"));
  }

  @Test public void markedQueryMapToList() {
    @NonNull final BriteDatabase<String> db = Objects.requireNonNull(this.db);

    @NonNull final TestObserver<MarkedValue<String, List<Employee>>> t = new TestObserver<>();
    db.createMarkedQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES)
            .mapToList(Employee.MARKED_MAPPER)
            .skip(1)
            .subscribe(t);

    db.insertMarked("marker", TABLE_EMPLOYEE, CONFLICT_NONE, employee("john", "John Johnson"));
    t.assertValue(new MarkedValue<>(
        "marker",
        Arrays.asList(
          new Employee("alice", "Alice Allison"), //
          new Employee("bob", "Bob Bobberson"), //
          new Employee("eve", "Eve Evenson"), //
          new Employee("john", "John Johnson") //
        )
      )
    );
  }

  @Test public void emptyMarkedQueryMapToOne() {
    @NonNull final BriteDatabase<String> db = Objects.requireNonNull(this.db);

    @NonNull final MarkedValue<String, Employee> employeeMarkedValue =
      db.createMarkedQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES + " LIMIT 1")
        .mapToOne(Employee.MARKED_MAPPER)
        .blockingFirst();
    assertThat(employeeMarkedValue.markers).isEmpty();
    assertThat(employeeMarkedValue.value).isEqualTo(new Employee("alice", "Alice Allison"));
  }

  @Test public void markedQueryMapToOne() {
    @NonNull final BriteDatabase<String> db = Objects.requireNonNull(this.db);

    @NonNull final TestObserver<MarkedValue<String, Employee>> t = new TestObserver<>();
    db.createMarkedQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES + " LIMIT 1")
            .mapToOne(Employee.MARKED_MAPPER)
            .skip(1)
            .subscribe(t);

    db.insertMarked("marker", TABLE_EMPLOYEE, CONFLICT_NONE, employee("john", "John Johnson"));
    t.assertValue(new MarkedValue<>("marker", new Employee("alice", "Alice Allison")));
  }

  @Test public void emptyMarkedQueryMapToOneOrDefault() {
    @NonNull final BriteDatabase<String> db = Objects.requireNonNull(this.db);

    @NonNull final MarkedValue<String, Employee> employeeMarkedValue =
      db.createMarkedQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES + " LIMIT 1")
        .mapToOneOrDefault(Employee.MARKED_MAPPER, new Employee("wrong", "Wrong Person"))
        .blockingFirst();
    assertThat(employeeMarkedValue.markers).isEmpty();
    assertThat(employeeMarkedValue.value).isEqualTo(new Employee("alice", "Alice Allison"));
  }

  @Test public void markedQueryMapToOneOrDefault() {
    @NonNull final BriteDatabase<String> db = Objects.requireNonNull(this.db);

    @NonNull final TestObserver<MarkedValue<String, Employee>> t = new TestObserver<>();
    db.createMarkedQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES + " LIMIT 1")
            .mapToOneOrDefault(Employee.MARKED_MAPPER, new Employee("wrong", "Wrong Person"))
            .skip(1)
            .subscribe(t);

    db.insertMarked("marker", TABLE_EMPLOYEE, CONFLICT_NONE, employee("john", "John Johnson"));
    t.assertValue(new MarkedValue<>("marker", new Employee("alice", "Alice Allison")));
  }

  @Test public void badMarkedQueryCallsError() {
    @NonNull final BriteDatabase<String> db = Objects.requireNonNull(this.db);

    // safeSubscribe is needed because the error occurs in onNext and will otherwise bubble up
    // to the thread exception handler.
    db.createMarkedQuery(TABLE_EMPLOYEE, "SELECT * FROM missing").safeSubscribe(o);
    o.assertEmptyMarkers(); // since we receive a proper onNext, we will receive a proper markers Set.
    o.assertErrorContains("no such table: missing");
  }

  @Test public void markedQueryWithArgs() {
    @NonNull final BriteDatabase<String> db = Objects.requireNonNull(this.db);

    db.createMarkedQuery(
        TABLE_EMPLOYEE, SELECT_EMPLOYEES + " WHERE " + USERNAME + " = ?", "bob")
        .subscribe(o);
    o.assertEmptyMarkers();
    o.assertCursor()
        .hasRow("bob", "Bob Bobberson")
        .isExhausted();
  }

  @Test public void markedQueryObservesInsertMarked() {
    @NonNull final BriteDatabase<String> db = Objects.requireNonNull(this.db);

    db.createMarkedQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES).subscribe(o);
    o.assertEmptyMarkers();
    o.assertCursor()
            .hasRow("alice", "Alice Allison")
            .hasRow("bob", "Bob Bobberson")
            .hasRow("eve", "Eve Evenson")
            .isExhausted();

    db.insertMarked("marker", TABLE_EMPLOYEE, CONFLICT_NONE, employee("john", "John Johnson"));
    o.assertMarkersEquals("marker");
    o.assertCursor()
            .hasRow("alice", "Alice Allison")
            .hasRow("bob", "Bob Bobberson")
            .hasRow("eve", "Eve Evenson")
            .hasRow("john", "John Johnson")
            .isExhausted();
  }

  @Test public void markedQueryInitialValueAndTriggerUsesScheduler() {
    @NonNull final BriteDatabase<String> db = Objects.requireNonNull(this.db);

    scheduler.runTasksImmediately(false);

    db.createMarkedQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES).subscribe(o);
    o.assertNoMoreEvents();
    scheduler.triggerActions();
    o.assertEmptyMarkers();
    o.assertCursor()
            .hasRow("alice", "Alice Allison")
            .hasRow("bob", "Bob Bobberson")
            .hasRow("eve", "Eve Evenson")
            .isExhausted();

    db.insertMarked("marker", TABLE_EMPLOYEE, CONFLICT_NONE, employee("john", "John Johnson"));
    o.assertNoMoreEvents();
    scheduler.triggerActions();
    o.assertMarkersEquals("marker");
    o.assertCursor()
            .hasRow("alice", "Alice Allison")
            .hasRow("bob", "Bob Bobberson")
            .hasRow("eve", "Eve Evenson")
            .hasRow("john", "John Johnson")
            .isExhausted();
  }

  @Test public void markedQueryNotNotifiedWhenInsertFails() {
    @NonNull final BriteDatabase<String> db = Objects.requireNonNull(this.db);

    db.createMarkedQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES).subscribe(o);
    o.assertEmptyMarkers();
    o.assertCursor()
        .hasRow("alice", "Alice Allison")
        .hasRow("bob", "Bob Bobberson")
        .hasRow("eve", "Eve Evenson")
        .isExhausted();

    db.insertMarked("marker", TABLE_EMPLOYEE, CONFLICT_IGNORE, employee("bob", "Bob Bobberson"));
    o.assertNoMoreEvents();
  }

  @Test public void markedQueryNotNotifiedWhenQueryTransformerUnsubscribes() {
    @NonNull final BriteDatabase<String> db = Objects.requireNonNull(this.db);

    db.createMarkedQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES).subscribe(o);
    o.assertEmptyMarkers();
    o.assertCursor()
        .hasRow("alice", "Alice Allison")
        .hasRow("bob", "Bob Bobberson")
        .hasRow("eve", "Eve Evenson")
        .isExhausted();

    killSwitch.onNext("kill");
    o.assertIsCompleted();

    db.insertMarked("marker", TABLE_EMPLOYEE, CONFLICT_NONE, employee("john", "John Johnson"));
    o.assertNoMoreEvents();
  }

  @Test public void markedQueryObservesUpdate() {
    @NonNull final BriteDatabase<String> db = Objects.requireNonNull(this.db);

    db.createMarkedQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES).subscribe(o);
    o.assertEmptyMarkers();
    o.assertCursor()
        .hasRow("alice", "Alice Allison")
        .hasRow("bob", "Bob Bobberson")
        .hasRow("eve", "Eve Evenson")
        .isExhausted();

    @NonNull final ContentValues values = new ContentValues();
    values.put(NAME, "Robert Bobberson");
    db.updateMarked("marker", TABLE_EMPLOYEE, CONFLICT_NONE, values, USERNAME + " = 'bob'");
    o.assertMarkersEquals("marker");
    o.assertCursor()
        .hasRow("alice", "Alice Allison")
        .hasRow("bob", "Robert Bobberson")
        .hasRow("eve", "Eve Evenson")
        .isExhausted();
  }

  @Test public void markedQueryNotNotifiedWhenUpdateAffectsZeroRows() {
    @NonNull final BriteDatabase<String> db = Objects.requireNonNull(this.db);

    db.createMarkedQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES).subscribe(o);
    o.assertEmptyMarkers();
    o.assertCursor()
        .hasRow("alice", "Alice Allison")
        .hasRow("bob", "Bob Bobberson")
        .hasRow("eve", "Eve Evenson")
        .isExhausted();

    @NonNull final ContentValues values = new ContentValues();
    values.put(NAME, "John Johnson");
    db.updateMarked("marker", TABLE_EMPLOYEE, CONFLICT_NONE, values, USERNAME + " = 'john'");
    o.assertNoMoreEvents();
  }

  @Test public void markerQueryObservesDeleteMarked() {
    @NonNull final BriteDatabase<String> db = Objects.requireNonNull(this.db);

    db.createMarkedQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES).subscribe(o);
    o.assertEmptyMarkers();
    o.assertCursor()
        .hasRow("alice", "Alice Allison")
        .hasRow("bob", "Bob Bobberson")
        .hasRow("eve", "Eve Evenson")
        .isExhausted();

    db.deleteMarked("marker", TABLE_EMPLOYEE, USERNAME + " = 'bob'");
    o.assertMarkersEquals("marker");
    o.assertCursor()
        .hasRow("alice", "Alice Allison")
        .hasRow("eve", "Eve Evenson")
        .isExhausted();
  }

  @Test public void markedQueryNotNotifiedWhenDeleteAffectsZeroRows() {
    @NonNull final BriteDatabase<String> db = Objects.requireNonNull(this.db);

    db.createMarkedQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES).subscribe(o);
    o.assertEmptyMarkers();
    o.assertCursor()
        .hasRow("alice", "Alice Allison")
        .hasRow("bob", "Bob Bobberson")
        .hasRow("eve", "Eve Evenson")
        .isExhausted();

    db.deleteMarked("marker", TABLE_EMPLOYEE, USERNAME + " = 'john'");
    o.assertNoMoreEvents();
  }

  @Test public void markedQueryMultipleTables() {
    @NonNull final BriteDatabase<String> db = Objects.requireNonNull(this.db);

    db.createMarkedQuery(BOTH_TABLES, SELECT_MANAGER_LIST).subscribe(o);
    o.assertEmptyMarkers();
    o.assertCursor()
        .hasRow("Eve Evenson", "Alice Allison")
        .isExhausted();
  }

  @Test public void markedQueryMultipleTablesWithQueryObject() {
    @NonNull final BriteDatabase<String> db = Objects.requireNonNull(this.db);

    db.createMarkedQuery(BOTH_TABLES, new SimpleSQLiteQuery(SELECT_MANAGER_LIST)).subscribe(o);
    o.assertEmptyMarkers();
    o.assertCursor()
        .hasRow("Eve Evenson", "Alice Allison")
        .isExhausted();
  }

  @Test public void markedQueryMultipleTablesObservesChanges() {
    @NonNull final BriteDatabase<String> db = Objects.requireNonNull(this.db);

    db.createMarkedQuery(BOTH_TABLES, SELECT_MANAGER_LIST).subscribe(o);
    o.assertEmptyMarkers();
    o.assertCursor()
        .hasRow("Eve Evenson", "Alice Allison")
        .isExhausted();

    // A new employee triggers, despite the fact that it's not in our result set.
    db.insertMarked("marker1", TABLE_EMPLOYEE, CONFLICT_NONE, employee("john", "John Johnson"));
    o.assertMarkersEquals("marker1");
    o.assertCursor()
        .hasRow("Eve Evenson", "Alice Allison")
        .isExhausted();

    // A new manager also triggers and it is in our result set.
    db.insertMarked("marker2", TABLE_MANAGER, CONFLICT_NONE, manager(testDb.bobId, testDb.eveId));
    o.assertMarkersEquals("marker2");
    o.assertCursor()
        .hasRow("Eve Evenson", "Alice Allison")
        .hasRow("Bob Bobberson", "Eve Evenson")
        .isExhausted();
  }

  @Test public void queryMultipleTablesObservesChangesOnlyOnce() {
    @NonNull final BriteDatabase<String> db = Objects.requireNonNull(this.db);

    // Employee table is in this list twice. We should still only be notified once for a change.
    @NonNull final List<String> tables = Arrays.asList(TABLE_EMPLOYEE, TABLE_MANAGER, TABLE_EMPLOYEE);
    db.createMarkedQuery(tables, SELECT_MANAGER_LIST).subscribe(o);
    o.assertEmptyMarkers();
    o.assertCursor()
        .hasRow("Eve Evenson", "Alice Allison")
        .isExhausted();

    @NonNull final ContentValues values = new ContentValues();
    values.put(NAME, "Even Evenson");
    db.updateMarked("marker", TABLE_EMPLOYEE, CONFLICT_NONE, values, USERNAME + " = 'eve'");
    o.assertMarkersEquals("marker");
    o.assertCursor()
        .hasRow("Even Evenson", "Alice Allison")
        .isExhausted();
  }

  @Test public void markedQueryNotNotifiedAfterDispose() {
    @NonNull final BriteDatabase<String> db = Objects.requireNonNull(this.db);

    db.createMarkedQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES).subscribe(o);
    o.assertEmptyMarkers();
    o.assertCursor()
        .hasRow("alice", "Alice Allison")
        .hasRow("bob", "Bob Bobberson")
        .hasRow("eve", "Eve Evenson")
        .isExhausted();
    o.dispose();

    db.insertMarked("marker", TABLE_EMPLOYEE, CONFLICT_NONE, employee("john", "John Johnson"));
    o.assertNoMoreEvents();
  }

  @Test public void markedQueryOnlyNotifiedAfterSubscribe() {
    @NonNull final BriteDatabase<String> db = Objects.requireNonNull(this.db);

    @NonNull final Observable<MarkedQuery<String>> markedQuery = db.createMarkedQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES);
    o.assertNoMoreEvents();

    db.insertMarked("marker", TABLE_EMPLOYEE, CONFLICT_NONE, employee("john", "John Johnson"));
    o.assertNoMoreEvents();

    markedQuery.subscribe(o);
    o.assertEmptyMarkers();
    o.assertCursor()
        .hasRow("alice", "Alice Allison")
        .hasRow("bob", "Bob Bobberson")
        .hasRow("eve", "Eve Evenson")
        .hasRow("john", "John Johnson")
        .isExhausted();
  }

  @Test public void executeSqlNoTrigger() {
    @NonNull final BriteDatabase<String> db = Objects.requireNonNull(this.db);

    db.createMarkedQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES)
        .skip(1) // Skip initial
        .subscribe(o);

    db.execute("UPDATE " + TABLE_EMPLOYEE + " SET " + NAME + " = 'Zach'");
    o.assertNoMoreEvents();
  }

  @Test public void executeSqlWithArgsNoTrigger() {
    @NonNull final BriteDatabase<String> db = Objects.requireNonNull(this.db);

    db.createMarkedQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES)
        .skip(1) // Skip initial
        .subscribe(o);

    db.execute("UPDATE " + TABLE_EMPLOYEE + " SET " + NAME + " = ?", "Zach");
    o.assertNoMoreEvents();
  }

  @Test public void executeAndTriggerMarked() {
    @NonNull final BriteDatabase<String> db = Objects.requireNonNull(this.db);

    db.createMarkedQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES).subscribe(o);
    o.assertEmptyMarkers();
    o.assertCursor()
        .hasRow("alice", "Alice Allison")
        .hasRow("bob", "Bob Bobberson")
        .hasRow("eve", "Eve Evenson")
        .isExhausted();

    db.executeAndTriggerMarked("marker", TABLE_EMPLOYEE,
            "UPDATE " + TABLE_EMPLOYEE + " SET " + NAME + " = 'Zach'");
    o.assertMarkersEquals("marker");
    o.assertCursor()
        .hasRow("alice", "Zach")
        .hasRow("bob", "Zach")
        .hasRow("eve", "Zach")
        .isExhausted();
  }

  @Test public void executeAndTriggerMarkedMultipleTables() {
    @NonNull final BriteDatabase<String> db = Objects.requireNonNull(this.db);

    db.createMarkedQuery(TABLE_MANAGER, SELECT_MANAGER_LIST).subscribe(o);
    o.assertEmptyMarkers();
    o.assertCursor()
        .hasRow("Eve Evenson", "Alice Allison")
        .isExhausted();
    @Nonnull final MarkedRecordingObserver employeeObserver = new MarkedRecordingObserver();
    db.createMarkedQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES).subscribe(employeeObserver);
    employeeObserver.assertEmptyMarkers();
    employeeObserver.assertCursor()
        .hasRow("alice", "Alice Allison")
        .hasRow("bob", "Bob Bobberson")
        .hasRow("eve", "Eve Evenson")
        .isExhausted();

    @NonNull final Set<String> tablesToTrigger = Collections.unmodifiableSet(new HashSet<>(BOTH_TABLES));
    db.executeAndTriggerMarked("marker", tablesToTrigger,
            "UPDATE " + TABLE_EMPLOYEE + " SET " + NAME + " = 'Zach'");

    o.assertMarkersEquals("marker");
    o.assertCursor()
        .hasRow("Zach", "Zach")
        .isExhausted();
    employeeObserver.assertMarkersEquals("marker");
    employeeObserver.assertCursor()
        .hasRow("alice", "Zach")
        .hasRow("bob", "Zach")
        .hasRow("eve", "Zach")
        .isExhausted();
  }

  @Test public void executeAndTriggerMarkedWithNoTables() {
    @NonNull final BriteDatabase<String> db = Objects.requireNonNull(this.db);

    db.createMarkedQuery(TABLE_MANAGER, SELECT_MANAGER_LIST).subscribe(o);
    o.assertEmptyMarkers();
    o.assertCursor()
        .hasRow("Eve Evenson", "Alice Allison")
        .isExhausted();

    db.executeAndTriggerMarked("marker", Collections.<String>emptySet(),
            "UPDATE " + TABLE_EMPLOYEE + " SET " + NAME + " = 'Zach'");

    o.assertNoMoreEvents();
  }

  @Test public void executeAndTriggerMarkedThrowsAndDoesNotTrigger() {
    @NonNull final BriteDatabase<String> db = Objects.requireNonNull(this.db);

    db.createMarkedQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES)
        .skip(1) // Skip initial
        .subscribe(o);

    try {
      db.executeAndTriggerMarked("marker", TABLE_EMPLOYEE,
              "UPDATE not_a_table SET " + NAME + " = 'Zach'");
      fail();
    } catch (SQLException ignored) {
    }
    o.assertNoMoreEvents();
  }

  @Test public void executeAndTriggerMarkedWithArgs() {
    @NonNull final BriteDatabase<String> db = Objects.requireNonNull(this.db);

    db.createMarkedQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES).subscribe(o);
    o.assertEmptyMarkers();
    o.assertCursor()
        .hasRow("alice", "Alice Allison")
        .hasRow("bob", "Bob Bobberson")
        .hasRow("eve", "Eve Evenson")
        .isExhausted();

    db.executeAndTriggerMarked("marker", TABLE_EMPLOYEE,
            "UPDATE " + TABLE_EMPLOYEE + " SET " + NAME + " = ?", "Zach");
    o.assertMarkersEquals("marker");
    o.assertCursor()
        .hasRow("alice", "Zach")
        .hasRow("bob", "Zach")
        .hasRow("eve", "Zach")
        .isExhausted();
  }

  @Test public void executeAndTriggerMarkedWithArgsThrowsAndDoesNotTrigger() {
    @NonNull final BriteDatabase<String> db = Objects.requireNonNull(this.db);

    db.createMarkedQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES)
        .skip(1) // Skip initial
        .subscribe(o);

    try {
      db.executeAndTriggerMarked("marker", TABLE_EMPLOYEE,
              "UPDATE not_a_table SET " + NAME + " = ?", "Zach");
      fail();
    } catch (SQLException ignored) {
    }
    o.assertNoMoreEvents();
  }

  @Test public void executeAndTriggerMarkedWithArgsWithMultipleTables() {
    @NonNull final BriteDatabase<String> db = Objects.requireNonNull(this.db);

    db.createMarkedQuery(TABLE_MANAGER, SELECT_MANAGER_LIST).subscribe(o);
    o.assertEmptyMarkers();
    o.assertCursor()
        .hasRow("Eve Evenson", "Alice Allison")
        .isExhausted();
    @NonNull final MarkedRecordingObserver employeeObserver = new MarkedRecordingObserver();
    db.createMarkedQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES).subscribe(employeeObserver);
    employeeObserver.assertEmptyMarkers();
    employeeObserver.assertCursor()
        .hasRow("alice", "Alice Allison")
        .hasRow("bob", "Bob Bobberson")
        .hasRow("eve", "Eve Evenson")
        .isExhausted();

    @NonNull final Set<String> tablesToTrigger = Collections.unmodifiableSet(new HashSet<>(BOTH_TABLES));
    db.executeAndTriggerMarked("marker", tablesToTrigger,
            "UPDATE " + TABLE_EMPLOYEE + " SET " + NAME + " = ?", "Zach");

    o.assertMarkersEquals("marker");
    o.assertCursor()
        .hasRow("Zach", "Zach")
        .isExhausted();
    employeeObserver.assertMarkersEquals("marker");
    employeeObserver.assertCursor()
        .hasRow("alice", "Zach")
        .hasRow("bob", "Zach")
        .hasRow("eve", "Zach")
        .isExhausted();
  }

  @Test public void executeAndTriggerMarkedWithArgsWithNoTables() {
    @NonNull final BriteDatabase<String> db = Objects.requireNonNull(this.db);

    db.createMarkedQuery(BOTH_TABLES, SELECT_MANAGER_LIST).subscribe(o);
    o.assertEmptyMarkers();
    o.assertCursor()
        .hasRow("Eve Evenson", "Alice Allison")
        .isExhausted();

    db.executeAndTriggerMarked("marker", Collections.<String>emptySet(),
            "UPDATE " + TABLE_EMPLOYEE + " SET " + NAME + " = ?", "Zach");

    o.assertNoMoreEvents();
  }

  @Test public void executeInsertMarkedAndTrigger() {
    @NonNull final BriteDatabase<String> db = Objects.requireNonNull(this.db);
    @NonNull final SupportSQLiteDatabase real = Objects.requireNonNull(this.real);

    @NonNull final SupportSQLiteStatement statement = real.compileStatement("INSERT INTO "
        + TABLE_EMPLOYEE + " (" + NAME + ", " + USERNAME + ") "
        + "VALUES ('Chad Chadson', 'chad')");

    db.createMarkedQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES).subscribe(o);
    o.assertEmptyMarkers();
    o.assertCursor()
        .hasRow("alice", "Alice Allison")
        .hasRow("bob", "Bob Bobberson")
        .hasRow("eve", "Eve Evenson")
        .isExhausted();

    db.executeInsertMarked("marker", TABLE_EMPLOYEE, statement);
    o.assertMarkersEquals("marker");
    o.assertCursor()
        .hasRow("alice", "Alice Allison")
        .hasRow("bob", "Bob Bobberson")
        .hasRow("eve", "Eve Evenson")
        .hasRow("chad", "Chad Chadson")
        .isExhausted();
  }

  @Test public void executeInsertMarkedAndDontTrigger() {
    @NonNull final BriteDatabase<String> db = Objects.requireNonNull(this.db);
    @NonNull final SupportSQLiteDatabase real = Objects.requireNonNull(this.real);

    @NonNull final SupportSQLiteStatement statement = real.compileStatement("INSERT OR IGNORE INTO "
        + TABLE_EMPLOYEE + " (" + NAME + ", " + USERNAME + ") "
        + "VALUES ('Alice Allison', 'alice')");

    db.createMarkedQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES).subscribe(o);
    o.assertEmptyMarkers();
    o.assertCursor()
        .hasRow("alice", "Alice Allison")
        .hasRow("bob", "Bob Bobberson")
        .hasRow("eve", "Eve Evenson")
        .isExhausted();

    db.executeInsertMarked("marker", TABLE_EMPLOYEE, statement);
    o.assertNoMoreEvents();
  }

  @Test public void executeInsertMarkedAndTriggerMultipleTables() {
    @NonNull final BriteDatabase<String> db = Objects.requireNonNull(this.db);
    @NonNull final SupportSQLiteDatabase real = Objects.requireNonNull(this.real);

    @NonNull final SupportSQLiteStatement statement = real.compileStatement("INSERT INTO "
        + TABLE_EMPLOYEE + " (" + NAME + ", " + USERNAME + ") "
        + "VALUES ('Chad Chadson', 'chad')");

    @NonNull final MarkedRecordingObserver managerObserver = new MarkedRecordingObserver();
    db.createMarkedQuery(TABLE_MANAGER, SELECT_MANAGER_LIST).subscribe(managerObserver);
    managerObserver.assertEmptyMarkers();
    managerObserver.assertCursor()
        .hasRow("Eve Evenson", "Alice Allison")
        .isExhausted();

    db.createMarkedQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES).subscribe(o);
    o.assertEmptyMarkers();
    o.assertCursor()
        .hasRow("alice", "Alice Allison")
        .hasRow("bob", "Bob Bobberson")
        .hasRow("eve", "Eve Evenson")
        .isExhausted();

    @NonNull final Set<String> employeeAndManagerTables = Collections.unmodifiableSet(new HashSet<>(
        BOTH_TABLES));
    db.executeInsertMarked("marker", employeeAndManagerTables, statement);

    o.assertMarkersEquals("marker");
    o.assertCursor()
        .hasRow("alice", "Alice Allison")
        .hasRow("bob", "Bob Bobberson")
        .hasRow("eve", "Eve Evenson")
        .hasRow("chad", "Chad Chadson")
        .isExhausted();
    managerObserver.assertMarkersEquals("marker");
    managerObserver.assertCursor()
        .hasRow("Eve Evenson", "Alice Allison")
        .isExhausted();
  }

  @Test public void executeInsertMarkerAndTriggerNoTables() {
    @NonNull final BriteDatabase<String> db = Objects.requireNonNull(this.db);
    @NonNull final SupportSQLiteDatabase real = Objects.requireNonNull(this.real);

    @NonNull final SupportSQLiteStatement statement = real.compileStatement("INSERT INTO "
        + TABLE_EMPLOYEE + " (" + NAME + ", " + USERNAME + ") "
        + "VALUES ('Chad Chadson', 'chad')");

    db.createMarkedQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES).subscribe(o);
    o.assertEmptyMarkers();
    o.assertCursor()
        .hasRow("alice", "Alice Allison")
        .hasRow("bob", "Bob Bobberson")
        .hasRow("eve", "Eve Evenson")
        .isExhausted();

    db.executeInsertMarked("marker", Collections.<String>emptySet(), statement);

    o.assertNoMoreEvents();
  }

  @Test public void executeInsertMarkedThrowsAndDoesNotTrigger() {
    @NonNull final BriteDatabase<String> db = Objects.requireNonNull(this.db);
    @NonNull final SupportSQLiteDatabase real = Objects.requireNonNull(this.real);

    @NonNull final SupportSQLiteStatement statement = real.compileStatement("INSERT INTO "
        + TABLE_EMPLOYEE + " (" + NAME + ", " + USERNAME + ") "
        + "VALUES ('Alice Allison', 'alice')");

    db.createMarkedQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES)
        .skip(1) // Skip initial
        .subscribe(o);

    try {
      db.executeInsertMarked("marker", TABLE_EMPLOYEE, statement);
      fail();
    } catch (SQLException ignored) {
    }
    o.assertNoMoreEvents();
  }

  @Test public void executeInsertMarkedWithArgsAndTrigger() {
    @NonNull final BriteDatabase<String> db = Objects.requireNonNull(this.db);
    @NonNull final SupportSQLiteDatabase real = Objects.requireNonNull(this.real);

    @NonNull final SupportSQLiteStatement statement = real.compileStatement("INSERT INTO "
        + TABLE_EMPLOYEE + " (" + NAME + ", " + USERNAME + ") VALUES (?, ?)");
    statement.bindString(1, "Chad Chadson");
    statement.bindString(2, "chad");

    db.createMarkedQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES).subscribe(o);
    o.assertEmptyMarkers();
    o.assertCursor()
        .hasRow("alice", "Alice Allison")
        .hasRow("bob", "Bob Bobberson")
        .hasRow("eve", "Eve Evenson")
        .isExhausted();

    db.executeInsertMarked("marker", TABLE_EMPLOYEE, statement);
    o.assertMarkersEquals("marker");
    o.assertCursor()
        .hasRow("alice", "Alice Allison")
        .hasRow("bob", "Bob Bobberson")
        .hasRow("eve", "Eve Evenson")
        .hasRow("chad", "Chad Chadson")
        .isExhausted();
  }

  @Test public void executeInsertMarkedWithArgsThrowsAndDoesNotTrigger() {
    @NonNull final BriteDatabase<String> db = Objects.requireNonNull(this.db);
    @NonNull final SupportSQLiteDatabase real = Objects.requireNonNull(this.real);

    @NonNull final SupportSQLiteStatement statement = real.compileStatement("INSERT INTO "
        + TABLE_EMPLOYEE + " (" + NAME + ", " + USERNAME + ") VALUES (?, ?)");
    statement.bindString(1, "Alice Aliison");
    statement.bindString(2, "alice");

    db.createMarkedQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES)
        .skip(1) // Skip initial
        .subscribe(o);

    try {
      db.executeInsertMarked("marker", TABLE_EMPLOYEE, statement);
      fail();
    } catch (SQLException ignored) {
    }
    o.assertNoMoreEvents();
  }

  @TargetApi(Build.VERSION_CODES.HONEYCOMB)
  @SdkSuppress(minSdkVersion = Build.VERSION_CODES.HONEYCOMB)
  @Test public void executeUpdateDeleteMarkedAndTrigger() {
    @NonNull final BriteDatabase<String> db = Objects.requireNonNull(this.db);
    @NonNull final SupportSQLiteDatabase real = Objects.requireNonNull(this.real);

    @NonNull final SupportSQLiteStatement statement = real.compileStatement(
        "UPDATE " + TABLE_EMPLOYEE + " SET " + NAME + " = 'Zach'");

    db.createMarkedQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES).subscribe(o);
    o.assertEmptyMarkers();
    o.assertCursor()
        .hasRow("alice", "Alice Allison")
        .hasRow("bob", "Bob Bobberson")
        .hasRow("eve", "Eve Evenson")
        .isExhausted();

    db.executeUpdateDeleteMarked("marker", TABLE_EMPLOYEE, statement);
    o.assertMarkersEquals("marker");
    o.assertCursor()
        .hasRow("alice", "Zach")
        .hasRow("bob", "Zach")
        .hasRow("eve", "Zach")
        .isExhausted();
  }

  @TargetApi(Build.VERSION_CODES.HONEYCOMB)
  @SdkSuppress(minSdkVersion = Build.VERSION_CODES.HONEYCOMB)
  @Test public void executeUpdateDeleteMarkedAndDontTrigger() {
    @NonNull final BriteDatabase<String> db = Objects.requireNonNull(this.db);
    @NonNull final SupportSQLiteDatabase real = Objects.requireNonNull(this.real);

    @NonNull final SupportSQLiteStatement statement = real.compileStatement(""
        + "UPDATE " + TABLE_EMPLOYEE
        + " SET " + NAME + " = 'Zach'"
        + " WHERE " + NAME + " = 'Rob'");

    db.createMarkedQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES).subscribe(o);
    o.assertEmptyMarkers();
    o.assertCursor()
        .hasRow("alice", "Alice Allison")
        .hasRow("bob", "Bob Bobberson")
        .hasRow("eve", "Eve Evenson")
        .isExhausted();

    db.executeUpdateDeleteMarked("marker", TABLE_EMPLOYEE, statement);
    o.assertNoMoreEvents();
  }

  @TargetApi(Build.VERSION_CODES.HONEYCOMB)
  @SdkSuppress(minSdkVersion = Build.VERSION_CODES.HONEYCOMB)
  @Test public void executeUpdateDeleteMarkedAndTriggerWithMultipleTables() {
    @NonNull final BriteDatabase<String> db = Objects.requireNonNull(this.db);
    @NonNull final SupportSQLiteDatabase real = Objects.requireNonNull(this.real);

    @NonNull final SupportSQLiteStatement statement = real.compileStatement(
        "UPDATE " + TABLE_EMPLOYEE + " SET " + NAME + " = 'Zach'");


    final MarkedRecordingObserver managerObserver = new MarkedRecordingObserver();
    db.createMarkedQuery(TABLE_MANAGER, SELECT_MANAGER_LIST).subscribe(managerObserver);
    managerObserver.assertEmptyMarkers();
    managerObserver.assertCursor()
        .hasRow("Eve Evenson", "Alice Allison")
        .isExhausted();

    db.createMarkedQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES).subscribe(o);
    o.assertEmptyMarkers();
    o.assertCursor()
        .hasRow("alice", "Alice Allison")
        .hasRow("bob", "Bob Bobberson")
        .hasRow("eve", "Eve Evenson")
        .isExhausted();

    @NonNull final Set<String> employeeAndManagerTables = Collections.unmodifiableSet(new HashSet<>(BOTH_TABLES));
    db.executeUpdateDeleteMarked("marker", employeeAndManagerTables, statement);

    o.assertMarkersEquals("marker");
    o.assertCursor()
        .hasRow("alice", "Zach")
        .hasRow("bob", "Zach")
        .hasRow("eve", "Zach")
        .isExhausted();
    managerObserver.assertMarkersEquals("marker");
    managerObserver.assertCursor()
        .hasRow("Zach", "Zach")
        .isExhausted();
  }

  @TargetApi(Build.VERSION_CODES.HONEYCOMB)
  @SdkSuppress(minSdkVersion = Build.VERSION_CODES.HONEYCOMB)
  @Test public void executeUpdateDeleteMarkedAndTriggerWithNoTables() {
    @NonNull final BriteDatabase<String> db = Objects.requireNonNull(this.db);
    @NonNull final SupportSQLiteDatabase real = Objects.requireNonNull(this.real);

    @NonNull final SupportSQLiteStatement statement = real.compileStatement(
        "UPDATE " + TABLE_EMPLOYEE + " SET " + NAME + " = 'Zach'");

    db.createMarkedQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES).subscribe(o);
    o.assertEmptyMarkers();
    o.assertCursor()
        .hasRow("alice", "Alice Allison")
        .hasRow("bob", "Bob Bobberson")
        .hasRow("eve", "Eve Evenson")
        .isExhausted();

    db.executeUpdateDeleteMarked("marker", Collections.<String>emptySet(), statement);

    o.assertNoMoreEvents();
  }

  @TargetApi(Build.VERSION_CODES.HONEYCOMB)
  @SdkSuppress(minSdkVersion = Build.VERSION_CODES.HONEYCOMB)
  @Test public void executeUpdateDeleteMarkedThrowsAndDoesNotTrigger() {
    @NonNull final BriteDatabase<String> db = Objects.requireNonNull(this.db);
    @NonNull final SupportSQLiteDatabase real = Objects.requireNonNull(this.real);

    @NonNull final SupportSQLiteStatement statement = real.compileStatement(
        "UPDATE " + TABLE_EMPLOYEE + " SET " + USERNAME + " = 'alice'");

    db.createMarkedQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES)
        .skip(1) // Skip initial
        .subscribe(o);

    try {
      db.executeUpdateDeleteMarked("marker", TABLE_EMPLOYEE, statement);
      fail();
    } catch (SQLException ignored) {
    }
    o.assertNoMoreEvents();
  }

  @TargetApi(Build.VERSION_CODES.HONEYCOMB)
  @SdkSuppress(minSdkVersion = Build.VERSION_CODES.HONEYCOMB)
  @Test public void executeUpdateDeleteMarkedWithArgsAndTrigger() {
    @NonNull final BriteDatabase<String> db = Objects.requireNonNull(this.db);
    @NonNull final SupportSQLiteDatabase real = Objects.requireNonNull(this.real);

    @NonNull final SupportSQLiteStatement statement = real.compileStatement(
        "UPDATE " + TABLE_EMPLOYEE + " SET " + NAME + " = ?");
    statement.bindString(1, "Zach");

    db.createMarkedQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES).subscribe(o);
    o.assertEmptyMarkers();
    o.assertCursor()
        .hasRow("alice", "Alice Allison")
        .hasRow("bob", "Bob Bobberson")
        .hasRow("eve", "Eve Evenson")
        .isExhausted();

    db.executeUpdateDeleteMarked("marker", TABLE_EMPLOYEE, statement);
    o.assertMarkersEquals("marker");
    o.assertCursor()
        .hasRow("alice", "Zach")
        .hasRow("bob", "Zach")
        .hasRow("eve", "Zach")
        .isExhausted();
  }

  @TargetApi(Build.VERSION_CODES.HONEYCOMB)
  @SdkSuppress(minSdkVersion = Build.VERSION_CODES.HONEYCOMB)
  @Test public void executeUpdateDeleteMarkedWithArgsThrowsAndDoesNotTrigger() {
    @NonNull final BriteDatabase<String> db = Objects.requireNonNull(this.db);
    @NonNull final SupportSQLiteDatabase real = Objects.requireNonNull(this.real);

    @NonNull final SupportSQLiteStatement statement = real.compileStatement(
        "UPDATE " + TABLE_EMPLOYEE + " SET " + USERNAME + " = ?");
    statement.bindString(1, "alice");

    db.createMarkedQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES)
        .skip(1) // Skip initial
        .subscribe(o);

    try {
      db.executeUpdateDeleteMarked("marker", TABLE_EMPLOYEE, statement);
      fail();
    } catch (SQLException ignored) {
    }
    o.assertNoMoreEvents();
  }

  @Test public void markedTransactionOnlyNotifiesOnce() {
    @NonNull final BriteDatabase<String> db = Objects.requireNonNull(this.db);

    db.createMarkedQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES).subscribe(o);
    o.assertEmptyMarkers();
    o.assertCursor()
            .hasRow("alice", "Alice Allison")
            .hasRow("bob", "Bob Bobberson")
            .hasRow("eve", "Eve Evenson")
            .isExhausted();

    @NonNull final Transaction<String> transaction = db.newTransaction();
    try {
      db.insertMarked("marker1", TABLE_EMPLOYEE, CONFLICT_NONE, employee("john", "John Johnson"));
      db.insertMarked("marker2", TABLE_EMPLOYEE, CONFLICT_NONE, employee("nick", "Nick Nickers"));
      o.assertNoMoreEvents();

      transaction.markSuccessful("marker3");
    } finally {
      transaction.end();
    }

    o.assertMarkersEquals("marker1", "marker2", "marker3");
    o.assertCursor()
            .hasRow("alice", "Alice Allison")
            .hasRow("bob", "Bob Bobberson")
            .hasRow("eve", "Eve Evenson")
            .hasRow("john", "John Johnson")
            .hasRow("nick", "Nick Nickers")
            .isExhausted();
  }

  @Test public void markedTransactionCanReceiveMarkedAndUnmarkedInserts() {
    @NonNull final BriteDatabase<String> db = Objects.requireNonNull(this.db);

    db.createMarkedQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES).subscribe(o);
    o.assertEmptyMarkers();
    o.assertCursor()
            .hasRow("alice", "Alice Allison")
            .hasRow("bob", "Bob Bobberson")
            .hasRow("eve", "Eve Evenson")
            .isExhausted();

    @NonNull final Transaction<String> transaction = db.newTransaction();
    try {
      db.insertMarked("marker1", TABLE_EMPLOYEE, CONFLICT_NONE, employee("john", "John Johnson"));
      db.insert(TABLE_EMPLOYEE, CONFLICT_NONE, employee("nick", "Nick Nickers"));
      o.assertNoMoreEvents();

      transaction.markSuccessful("marker2");
    } finally {
      transaction.end();
    }

    o.assertMarkersEquals("marker1", "marker2");
    o.assertCursor()
            .hasRow("alice", "Alice Allison")
            .hasRow("bob", "Bob Bobberson")
            .hasRow("eve", "Eve Evenson")
            .hasRow("john", "John Johnson")
            .hasRow("nick", "Nick Nickers")
            .isExhausted();
  }

  @Test public void markedTransactionCanReceiveUnmarkedInsert() {
    @NonNull final BriteDatabase<String> db = Objects.requireNonNull(this.db);

    db.createMarkedQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES).subscribe(o);
    o.assertEmptyMarkers();
    o.assertCursor()
            .hasRow("alice", "Alice Allison")
            .hasRow("bob", "Bob Bobberson")
            .hasRow("eve", "Eve Evenson")
            .isExhausted();

    @NonNull final Transaction<String> transaction = db.newTransaction();
    try {
      db.insert(TABLE_EMPLOYEE, CONFLICT_NONE, employee("john", "John Johnson"));
      o.assertNoMoreEvents();

      transaction.markSuccessful("marker");
    } finally {
      transaction.end();
    }

    o.assertMarkersEquals("marker");
    o.assertCursor()
            .hasRow("alice", "Alice Allison")
            .hasRow("bob", "Bob Bobberson")
            .hasRow("eve", "Eve Evenson")
            .hasRow("john", "John Johnson")
            .isExhausted();
  }

  @Test public void unmarkedTransactionCanReceiveMarkedAndUnmarkedInserts() {
    @NonNull final BriteDatabase<String> db = Objects.requireNonNull(this.db);

    db.createMarkedQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES).subscribe(o);
    o.assertEmptyMarkers();
    o.assertCursor()
            .hasRow("alice", "Alice Allison")
            .hasRow("bob", "Bob Bobberson")
            .hasRow("eve", "Eve Evenson")
            .isExhausted();

    @NonNull final Transaction<String> transaction = db.newTransaction();
    try {
      db.insertMarked("marker", TABLE_EMPLOYEE, CONFLICT_NONE, employee("john", "John Johnson"));
      db.insert(TABLE_EMPLOYEE, CONFLICT_NONE, employee("nick", "Nick Nickers"));
      o.assertNoMoreEvents();

      transaction.markSuccessful();
    } finally {
      transaction.end();
    }

    o.assertMarkersEquals("marker");
    o.assertCursor()
            .hasRow("alice", "Alice Allison")
            .hasRow("bob", "Bob Bobberson")
            .hasRow("eve", "Eve Evenson")
            .hasRow("john", "John Johnson")
            .hasRow("nick", "Nick Nickers")
            .isExhausted();
  }

  @Test public void markedTransactionCreatedFromTransactionNotificationWorks() {
    @NonNull final BriteDatabase<String> db = Objects.requireNonNull(this.db);

    // Tests the case where a transaction is created in the subscriber to a query which gets
    // notified as the result of another transaction being committed. With improper ordering, this
    // can result in creating a new transaction before the old is committed on the underlying DB.

    db.createMarkedQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES)
        .subscribe(new Consumer<MarkedQuery<String>>() {
          @Override public void accept(MarkedQuery<String> markedQuery) {
            db.newTransaction().end();
          }
        }).isDisposed();

    @NonNull final Transaction<String> transaction = db.newTransaction();
    try {
      db.insertMarked("marker1", TABLE_EMPLOYEE, CONFLICT_NONE, employee("john", "John Johnson"));
      transaction.markSuccessful("marker2");
    } finally {
      transaction.end();
    }
  }

  @Test public void markedTransactionIsCloseable() throws IOException {
    @NonNull final BriteDatabase<String> db = Objects.requireNonNull(this.db);

    db.createMarkedQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES).subscribe(o);
    o.assertEmptyMarkers();
    o.assertCursor()
        .hasRow("alice", "Alice Allison")
        .hasRow("bob", "Bob Bobberson")
        .hasRow("eve", "Eve Evenson")
        .isExhausted();

    @NonNull final Transaction<String> transaction = db.newTransaction();
    @NonNull final Closeable closeableTransaction = transaction; // Verify type is implemented.
    try {
      db.insertMarked("marker1", TABLE_EMPLOYEE, CONFLICT_NONE, employee("john", "John Johnson"));
      db.insertMarked("marker2", TABLE_EMPLOYEE, CONFLICT_NONE, employee("nick", "Nick Nickers"));
      transaction.markSuccessful("marker3");
    } finally {
      closeableTransaction.close();
    }

    o.assertMarkersEquals("marker1", "marker2", "marker3");
    o.assertCursor()
        .hasRow("alice", "Alice Allison")
        .hasRow("bob", "Bob Bobberson")
        .hasRow("eve", "Eve Evenson")
        .hasRow("john", "John Johnson")
        .hasRow("nick", "Nick Nickers")
        .isExhausted();
  }

  @Test public void markedTransactionDoesNotThrow() {
    @NonNull final BriteDatabase<String> db = Objects.requireNonNull(this.db);

    db.createMarkedQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES).subscribe(o);
    o.assertEmptyMarkers();
    o.assertCursor()
        .hasRow("alice", "Alice Allison")
        .hasRow("bob", "Bob Bobberson")
        .hasRow("eve", "Eve Evenson")
        .isExhausted();

    @NonNull final Transaction<String> transaction = db.newTransaction();
    try {
      db.insertMarked("marker1", TABLE_EMPLOYEE, CONFLICT_NONE, employee("john", "John Johnson"));
      db.insertMarked("marker2", TABLE_EMPLOYEE, CONFLICT_NONE, employee("nick", "Nick Nickers"));
      transaction.markSuccessful("marker3");
    } finally {
      transaction.close(); // Transactions should not throw on close().
    }

    o.assertMarkersEquals("marker1", "marker2", "marker3");
    o.assertCursor()
        .hasRow("alice", "Alice Allison")
        .hasRow("bob", "Bob Bobberson")
        .hasRow("eve", "Eve Evenson")
        .hasRow("john", "John Johnson")
        .hasRow("nick", "Nick Nickers")
        .isExhausted();
  }

  @Test public void markedQueryCreatedDuringTransactionThrows() {
    @NonNull final BriteDatabase<String> db = Objects.requireNonNull(this.db);

    //noinspection CheckResult
    db.newTransaction();
    try {
      //noinspection CheckResult
      db.createMarkedQuery(TABLE_EMPLOYEE, "marker", SELECT_EMPLOYEES);
      fail();
    } catch (IllegalStateException e) {
      assertThat(e.getMessage()).startsWith("Cannot create observable markedQuery in transaction.");
    }
  }

  @Test public void markedQuerySubscribedToDuringTransactionThrows() {
    @NonNull final BriteDatabase<String> db = Objects.requireNonNull(this.db);

    @NonNull final Observable<MarkedQuery<String>> markedQuery = db.createMarkedQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES);

    db.newTransaction();
    markedQuery.subscribe(o);
    o.assertErrorContains("Cannot subscribe to observable query in a transaction.");
  }

  @Test public void callingEndMultipleTimesThrows() {
    @NonNull final BriteDatabase<String> db = Objects.requireNonNull(this.db);

    @NonNull final Transaction<String> transaction = db.newTransaction();
    transaction.end();
    try {
      transaction.end();
      fail();
    } catch (IllegalStateException e) {
      assertThat(e).hasMessageThat().isEqualTo("Not in transaction.");
    }
  }

  @Test public void markedQuerySubscribedToDuringTransactionOnDifferentThread()
      throws InterruptedException {
    @NonNull final BriteDatabase<String> db = Objects.requireNonNull(this.db);

    @NonNull final Transaction<String> transaction = db.newTransaction();

    @NonNull final CountDownLatch latch = new CountDownLatch(1);
    new Thread() {
      @Override public void run() {
        db.createMarkedQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES).subscribe(o);
        latch.countDown();
      }
    }.start();

    Thread.sleep(500); // Wait for the thread to block on initial query.
    // MarkedRecordingObserver receives a MarkedQuery, which includes the empty markers Set
    // it will then get blocked when it attempts to run the query.
    o.assertEmptyMarkers();
    o.assertNoMoreEvents();

    transaction.end(); // Allow other queries to continue.
    latch.await(500, MILLISECONDS); // Wait for thread to observe initial query.

    // since we were previously blocked waiting for the query to run
    // we should now have query results. We thus won't have another markers Set.
    o.assertCursor()
        .hasRow("alice", "Alice Allison")
        .hasRow("bob", "Bob Bobberson")
        .hasRow("eve", "Eve Evenson")
        .isExhausted();
  }

  @Test public void markedQueryCreatedBeforeMarkedTransactionButSubscribedAfter() {
    @NonNull final BriteDatabase<String> db = Objects.requireNonNull(this.db);

    @NonNull final Observable<MarkedQuery<String>> markedQuery = db.createMarkedQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES);

    @NonNull final Transaction<String> transaction = db.newTransaction();
    try {
      db.insertMarked("marker1", TABLE_EMPLOYEE, CONFLICT_NONE, employee("john", "John Johnson"));
      db.insertMarked("marker2", TABLE_EMPLOYEE, CONFLICT_NONE, employee("nick", "Nick Nickers"));
      transaction.markSuccessful("marker3");
    } finally {
      transaction.end();
    }

    markedQuery.subscribe(o);
    o.assertEmptyMarkers(); // empty markers because we weren't subscribed when the inserts happened
    o.assertCursor()
        .hasRow("alice", "Alice Allison")
        .hasRow("bob", "Bob Bobberson")
        .hasRow("eve", "Eve Evenson")
        .hasRow("john", "John Johnson")
        .hasRow("nick", "Nick Nickers")
        .isExhausted();
  }

  @Test public void synchronousQueryDuringMarkedTransaction() {
    @NonNull final BriteDatabase<String> db = Objects.requireNonNull(this.db);

    @NonNull final Transaction<String> transaction = db.newTransaction();
    try {
      transaction.markSuccessful("marker");

      assertCursor(db.query(SELECT_EMPLOYEES))
          .hasRow("alice", "Alice Allison")
          .hasRow("bob", "Bob Bobberson")
          .hasRow("eve", "Eve Evenson")
          .isExhausted();
    } finally {
      transaction.end();
    }
  }

  @Test public void synchronousQueryDuringMarkedTransactionSeesChanges() {
    @NonNull final BriteDatabase<String> db = Objects.requireNonNull(this.db);

    @NonNull final Transaction<String> transaction = db.newTransaction();
    try {
      assertCursor(db.query(SELECT_EMPLOYEES))
          .hasRow("alice", "Alice Allison")
          .hasRow("bob", "Bob Bobberson")
          .hasRow("eve", "Eve Evenson")
          .isExhausted();

      db.insertMarked("marker1", TABLE_EMPLOYEE, CONFLICT_NONE, employee("john", "John Johnson"));
      db.insertMarked("marker2", TABLE_EMPLOYEE, CONFLICT_NONE, employee("nick", "Nick Nickers"));

      assertCursor(db.query(SELECT_EMPLOYEES))
          .hasRow("alice", "Alice Allison")
          .hasRow("bob", "Bob Bobberson")
          .hasRow("eve", "Eve Evenson")
          .hasRow("john", "John Johnson")
          .hasRow("nick", "Nick Nickers")
          .isExhausted();

      transaction.markSuccessful("marker3");
    } finally {
      transaction.end();
    }
  }

  @Test public void synchronousQueryWithSupportSQLiteQueryDuringMarkedTransaction() {
    @NonNull final BriteDatabase<String> db = Objects.requireNonNull(this.db);

    @NonNull final Transaction<String> transaction = db.newTransaction();
    try {
      transaction.markSuccessful("marker");
      assertCursor(db.query(new SimpleSQLiteQuery(SELECT_EMPLOYEES)))
              .hasRow("alice", "Alice Allison")
              .hasRow("bob", "Bob Bobberson")
              .hasRow("eve", "Eve Evenson")
              .isExhausted();
    } finally {
      transaction.end();
    }
  }

  @Test public void synchronousQueryWithSupportSQLiteQueryDuringMarkedTransactionSeesChanges() {
    @NonNull final BriteDatabase<String> db = Objects.requireNonNull(this.db);

    @NonNull final Transaction<String> transaction = db.newTransaction();
    try {
      assertCursor(db.query(new SimpleSQLiteQuery(SELECT_EMPLOYEES)))
              .hasRow("alice", "Alice Allison")
              .hasRow("bob", "Bob Bobberson")
              .hasRow("eve", "Eve Evenson")
              .isExhausted();

      db.insertMarked("marker1", TABLE_EMPLOYEE, CONFLICT_NONE, employee("john", "John Johnson"));
      db.insertMarked("marker2", TABLE_EMPLOYEE, CONFLICT_NONE, employee("nick", "Nick Nickers"));

      assertCursor(db.query(new SimpleSQLiteQuery(SELECT_EMPLOYEES)))
              .hasRow("alice", "Alice Allison")
              .hasRow("bob", "Bob Bobberson")
              .hasRow("eve", "Eve Evenson")
              .hasRow("john", "John Johnson")
              .hasRow("nick", "Nick Nickers")
              .isExhausted();

      transaction.markSuccessful("marker3");
    } finally {
      transaction.end();
    }
  }

  @Test public void nestedMarkedTransactionsOnlyNotifyOnce() {
    @NonNull final BriteDatabase<String> db = Objects.requireNonNull(this.db);

    db.createMarkedQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES).subscribe(o);
    o.assertEmptyMarkers();
    o.assertCursor()
        .hasRow("alice", "Alice Allison")
        .hasRow("bob", "Bob Bobberson")
        .hasRow("eve", "Eve Evenson")
        .isExhausted();

    @NonNull final Transaction<String> transactionOuter = db.newTransaction();
    try {
      db.insertMarked("marker1", TABLE_EMPLOYEE, CONFLICT_NONE, employee("john", "John Johnson"));

      @NonNull final Transaction<String> transactionInner = db.newTransaction();
      try {
        db.insertMarked("marker2", TABLE_EMPLOYEE, CONFLICT_NONE, employee("nick", "Nick Nickers"));
        transactionInner.markSuccessful("marker3");
      } finally {
        transactionInner.end();
      }

      transactionOuter.markSuccessful("marker4");
    } finally {
      transactionOuter.end();
    }

    o.assertMarkersEquals("marker1", "marker2", "marker3", "marker4");
    o.assertCursor()
        .hasRow("alice", "Alice Allison")
        .hasRow("bob", "Bob Bobberson")
        .hasRow("eve", "Eve Evenson")
        .hasRow("john", "John Johnson")
        .hasRow("nick", "Nick Nickers")
        .isExhausted();
  }

  @Test public void nestedMarkedTransactionsOnMultipleTables() {
    @NonNull final BriteDatabase<String> db = Objects.requireNonNull(this.db);

    db.createMarkedQuery(BOTH_TABLES, SELECT_MANAGER_LIST).subscribe(o);
    o.assertEmptyMarkers();
    o.assertCursor()
        .hasRow("Eve Evenson", "Alice Allison")
        .isExhausted();

    @NonNull final Transaction<String> transactionOuter = db.newTransaction();
    try {

      @NonNull final Transaction<String> transactionInner1 = db.newTransaction();
      try {
        db.insertMarked("marker1", TABLE_EMPLOYEE, CONFLICT_NONE, employee("john", "John Johnson"));
        transactionInner1.markSuccessful("marker2");
      } finally {
        transactionInner1.end();
      }

      @NonNull final Transaction<String> transactionInner2 = db.newTransaction();
      try {
        db.insertMarked("marker3", TABLE_MANAGER, CONFLICT_NONE, manager(testDb.aliceId, testDb.bobId));
        transactionInner2.markSuccessful("marker4");
      } finally {
        transactionInner2.end();
      }

      transactionOuter.markSuccessful();
    } finally {
      transactionOuter.end();
    }

    o.assertMarkersEquals("marker1", "marker2", "marker3", "marker4");
    o.assertCursor()
        .hasRow("Eve Evenson", "Alice Allison")
        .hasRow("Alice Allison", "Bob Bobberson")
        .isExhausted();
  }

  @Test public void emptyMarkedTransactionDoesNotNotify() {
    @NonNull final BriteDatabase<String> db = Objects.requireNonNull(this.db);

    db.createMarkedQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES).subscribe(o);
    o.assertEmptyMarkers();
    o.assertCursor()
        .hasRow("alice", "Alice Allison")
        .hasRow("bob", "Bob Bobberson")
        .hasRow("eve", "Eve Evenson")
        .isExhausted();

    @NonNull final Transaction<String> transaction = db.newTransaction();
    try {
      transaction.markSuccessful("marker");
    } finally {
      transaction.end();
    }
    o.assertNoMoreEvents();
  }

  @Test public void markedTransactionRollbackDoesNotNotify() {
    @NonNull final BriteDatabase<String> db = Objects.requireNonNull(this.db);

    db.createMarkedQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES).subscribe(o);
    o.assertEmptyMarkers();
    o.assertCursor()
        .hasRow("alice", "Alice Allison")
        .hasRow("bob", "Bob Bobberson")
        .hasRow("eve", "Eve Evenson")
        .isExhausted();

    @NonNull final Transaction<String> transaction = db.newTransaction();
    try {
      db.insertMarked("marker1", TABLE_EMPLOYEE, CONFLICT_NONE, employee("john", "John Johnson"));
      db.insertMarked("marker2", TABLE_EMPLOYEE, CONFLICT_NONE, employee("nick", "Nick Nickers"));
      // No call to set successful.
    } finally {
      transaction.end();
    }
    o.assertNoMoreEvents();
  }

  @TargetApi(Build.VERSION_CODES.HONEYCOMB)
  @SdkSuppress(minSdkVersion = Build.VERSION_CODES.HONEYCOMB)
  @Test public void nonExclusiveMarkedTransactionWorks() throws InterruptedException {
    @NonNull final BriteDatabase<String> db = Objects.requireNonNull(this.db);

    @NonNull final CountDownLatch transactionStarted = new CountDownLatch(1);
    @NonNull final CountDownLatch transactionProceed = new CountDownLatch(1);
    @NonNull final CountDownLatch transactionCompleted = new CountDownLatch(1);

    new Thread() {
      @Override public void run() {
        @NonNull final Transaction<String> transaction = db.newNonExclusiveTransaction();
        transactionStarted.countDown();
        try {
          db.insertMarked("marker1", TABLE_EMPLOYEE, CONFLICT_NONE, employee("hans", "Hans Hanson"));
          transactionProceed.await(10, SECONDS);
        } catch (InterruptedException e) {
          throw new RuntimeException("Exception in transaction thread", e);
        }
        transaction.markSuccessful("marker2");
        transaction.close();
        transactionCompleted.countDown();
      }
    }.start();

    assertThat(transactionStarted.await(10, SECONDS)).isTrue();

    //Simple query
    @NonNull final MarkedValue<String, Employee> employeeMarkedValue =
        db.createMarkedQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES + " LIMIT 1")
            .lift(MarkedQuery.mapToOne(Employee.MARKED_MAPPER))
            .blockingFirst();
    assertThat(employeeMarkedValue.markers).isEmpty();
    assertThat(employeeMarkedValue.value).isEqualTo(new Employee("alice", "Alice Allison"));

    transactionProceed.countDown();
    assertThat(transactionCompleted.await(10, SECONDS)).isTrue();
  }

  @Test public void badQueryThrows() {
    @NonNull final BriteDatabase<String> db = Objects.requireNonNull(this.db);

    try {
      //noinspection CheckResult
      db.query("SELECT * FROM missing");
      fail();
    } catch (SQLiteException e) {
      assertThat(e.getMessage()).contains("no such table: missing");
    }
  }

  @Test public void badInsertMarkedThrows() {
    @NonNull final BriteDatabase<String> db = Objects.requireNonNull(this.db);

    try {
      db.insertMarked("marker", "missing", CONFLICT_NONE, employee("john", "John Johnson"));
      fail();
    } catch (SQLiteException e) {
      assertThat(e.getMessage()).contains("no such table: missing");
    }
  }

  @Test public void badUpdateMarkedThrows() {
    @NonNull final BriteDatabase<String> db = Objects.requireNonNull(this.db);

    try {
      db.updateMarked("marker", "missing", CONFLICT_NONE, employee("john", "John Johnson"), "1");
      fail();
    } catch (SQLiteException e) {
      assertThat(e.getMessage()).contains("no such table: missing");
    }
  }

  @Test public void badDeleteMarkedThrows() {
    @NonNull final BriteDatabase<String> db = Objects.requireNonNull(this.db);

    try {
      db.deleteMarked("marked", "missing", "1");
      fail();
    } catch (SQLiteException e) {
      assertThat(e.getMessage()).contains("no such table: missing");
    }
  }

  @NonNull
  private static CursorAssert assertCursor(@NonNull Cursor cursor) {
    return new CursorAssert(cursor);
  }
}
