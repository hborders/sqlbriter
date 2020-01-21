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

import android.database.Cursor;
import android.os.Build;

import androidx.annotation.NonNull;
import androidx.annotation.Nullable;
import androidx.sqlite.db.SupportSQLiteOpenHelper;
import androidx.sqlite.db.SupportSQLiteOpenHelper.Configuration;
import androidx.sqlite.db.SupportSQLiteOpenHelper.Factory;
import androidx.sqlite.db.framework.FrameworkSQLiteOpenHelperFactory;
import androidx.test.filters.SdkSuppress;
import androidx.test.platform.app.InstrumentationRegistry;

import com.stealthmountain.sqldim.SqlDim.MarkedQuery;
import com.stealthmountain.sqldim.TestDb.Employee;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import io.reactivex.Observable;
import io.reactivex.functions.BiFunction;
import io.reactivex.observers.TestObserver;
import io.reactivex.schedulers.Schedulers;

import static android.database.sqlite.SQLiteDatabase.CONFLICT_NONE;
import static com.google.common.truth.Truth.assertThat;
import static com.stealthmountain.sqldim.SqlDim.MarkedQuery.MarkedValue;
import static com.stealthmountain.sqldim.TestDb.Employee.MARKED_MAPPER;
import static com.stealthmountain.sqldim.TestDb.SELECT_EMPLOYEES;
import static com.stealthmountain.sqldim.TestDb.TABLE_EMPLOYEE;
import static com.stealthmountain.sqldim.TestDb.employee;
import static org.junit.Assert.fail;

public final class MarkedQueryTest {
  @Nullable private DimDatabase<String> db;

  @Before public void setUp() {
    @NonNull final Configuration configuration = Configuration.builder(
            InstrumentationRegistry.getInstrumentation().getTargetContext()
    )
        .callback(new TestDb())
        .build();

    @NonNull final Factory factory = new FrameworkSQLiteOpenHelperFactory();
    @NonNull final SupportSQLiteOpenHelper helper = factory.create(configuration);

    @NonNull final SqlDim<String> sqlDim = new SqlDim.Builder<String>().build();
    db = sqlDim.wrapDatabaseHelper(helper, Schedulers.trampoline());
  }

  @Test public void emptyMarkedQueryMapToOne() {
    @NonNull final DimDatabase<String> db = Objects.requireNonNull(this.db);

    @NonNull final MarkedValue<String, Employee> employeeMarkedValue =
        db.createMarkedQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES + " LIMIT 1")
            .lift(MarkedQuery.mapToOne(MARKED_MAPPER))
            .blockingFirst();
    assertThat(employeeMarkedValue).isEqualTo(new MarkedValue<>(new Employee("alice", "Alice Allison")));
  }

  @Test public void markedQueryMapToOne() {
    @NonNull final DimDatabase<String> db = Objects.requireNonNull(this.db);

    TestObserver<MarkedValue<String, Employee>> t = new TestObserver<>();
    db.createMarkedQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES + " LIMIT 1")
            .lift(MarkedQuery.mapToOne(Employee.MARKED_MAPPER))
            .skip(1)
            .subscribe(t);

    db.insertMarked("marker", TABLE_EMPLOYEE, CONFLICT_NONE, employee("john", "John Johnson"));

    t.assertValue(new MarkedValue<>("marker", new Employee("alice", "Alice Allison")));
  }

  @Test public void mapToOneThrowsWhenMapperReturnsNull() {
    @NonNull final DimDatabase<String> db = Objects.requireNonNull(this.db);

    db.createMarkedQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES + " LIMIT 1")
        .lift(MarkedQuery.mapToOne(new BiFunction<Cursor, Set<String>, Employee>() {
          @NonNull @Override public Employee apply(@NonNull Cursor cursor, @NonNull Set<String> markers) {
            //noinspection ConstantConditions
            return null;
          }
        }))
        .test()
        .assertError(NullPointerException.class)
        .assertErrorMessage("QueryToOne mapper returned null");
  }

  @Test public void mapToOneThrowsOnMultipleRows() {
    @NonNull final DimDatabase<String> db = Objects.requireNonNull(this.db);

    @NonNull final Observable<MarkedValue<String, Employee>> employeesMarkedValue =
        db.createMarkedQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES + " LIMIT 2") //
            .lift(MarkedQuery.mapToOne(MARKED_MAPPER));
    try {
      //noinspection ResultOfMethodCallIgnored
      employeesMarkedValue.blockingFirst();
      fail();
    } catch (IllegalStateException e) {
      assertThat(e).hasMessageThat().isEqualTo("Cursor returned more than 1 row");
    }
  }

  @Test public void mapToOneIgnoresNullCursor() {
    @NonNull final MarkedQuery<String> nully = new MarkedQuery<String>(Collections.emptySet()) {
      @Nullable @Override public Cursor run() {
        return null;
      }
    };

    @NonNull final TestObserver<MarkedValue<String, Employee>> observer = new TestObserver<>();
    Observable.just(nully)
        .lift(MarkedQuery.mapToOne(MARKED_MAPPER))
        .subscribe(observer);

    observer.assertNoValues();
    observer.assertComplete();
  }

  @Test public void emptyMarkedMapToOneOrDefault() {
    @NonNull final DimDatabase<String> db = Objects.requireNonNull(this.db);

    @NonNull final MarkedValue<String, Employee> employeeMarkedValue =
        db.createMarkedQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES + " LIMIT 1")
            .lift(MarkedQuery.mapToOneOrDefault(MARKED_MAPPER, new Employee("fred", "Fred Frederson")))
            .blockingFirst();
    assertThat(employeeMarkedValue).isEqualTo(new MarkedValue<>(new Employee("alice", "Alice Allison")));
  }

  @Test public void markedMapToOneOrDefault() {
    @NonNull final DimDatabase<String> db = Objects.requireNonNull(this.db);

    TestObserver<MarkedValue<String, Employee>> t = new TestObserver<>();
    db.createMarkedQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES + " LIMIT 1")
            .lift(MarkedQuery.mapToOneOrDefault(MARKED_MAPPER, new Employee("fred", "Fred Frederson")))
            .skip(1)
            .subscribe(t);

    db.insertMarked("marker", TABLE_EMPLOYEE, CONFLICT_NONE, employee("john", "John Johnson"));

    t.assertValue(new MarkedValue<>("marker", new Employee("alice", "Alice Allison")));
  }

  @Test public void mapToOneOrDefaultDisallowsNullDefault() {
    try {
      //noinspection ConstantConditions
      MarkedQuery.mapToOneOrDefault(MARKED_MAPPER, null);
      fail();
    } catch (NullPointerException e) {
      assertThat(e).hasMessageThat().isEqualTo("defaultValue == null");
    }
  }

  @Test public void mapToOneOrDefaultThrowsWhenMapperReturnsNull() {
    @NonNull final DimDatabase<String> db = Objects.requireNonNull(this.db);

    db.createMarkedQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES + " LIMIT 1")
        .lift(MarkedQuery.mapToOneOrDefault(new BiFunction<Cursor, Set<String>, Employee>() {
          @NonNull @Override public Employee apply(@NonNull Cursor cursor, @NonNull Set<String> markers) {
            //noinspection ConstantConditions
            return null;
          }
        }, new Employee("fred", "Fred Frederson")))
        .test()
        .assertError(NullPointerException.class)
        .assertErrorMessage("QueryToOne mapper returned null");
  }

  @Test public void mapToOneOrDefaultThrowsOnMultipleRows() {
    @NonNull final DimDatabase<String> db = Objects.requireNonNull(this.db);

    @NonNull final Observable<MarkedValue<String, Employee>> employees =
        db.createMarkedQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES + " LIMIT 2") //
            .lift(MarkedQuery.mapToOneOrDefault(MARKED_MAPPER, new Employee("fred", "Fred Frederson")));
    try {
      //noinspection ResultOfMethodCallIgnored
      employees.blockingFirst();
      fail();
    } catch (IllegalStateException e) {
      assertThat(e).hasMessageThat().isEqualTo("Cursor returned more than 1 row");
    }
  }

  @Test public void mapToOneOrDefaultReturnsDefaultWhenNullCursor() {
    @NonNull final Employee defaultEmployee = new Employee("bob", "Bob Bobberson");
    @NonNull final MarkedQuery<String> nully = new MarkedQuery<String>(Collections.emptySet()) {
      @Nullable @Override public Cursor run() {
        return null;
      }
    };

    @NonNull final TestObserver<MarkedValue<String, Employee>> observer = new TestObserver<>();
    Observable.just(nully)
        .lift(MarkedQuery.mapToOneOrDefault(MARKED_MAPPER, defaultEmployee))
        .subscribe(observer);

    observer.assertValueSequence(Collections.singletonList(new MarkedValue<>(defaultEmployee)));
    observer.assertComplete();
  }

  @Test public void emptyMarkedMapToList() {
    @NonNull final DimDatabase<String> db = Objects.requireNonNull(this.db);

    @NonNull final MarkedValue<String, List<Employee>> employees =
        db.createMarkedQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES)
            .lift(MarkedQuery.mapToList(MARKED_MAPPER))
            .blockingFirst();
    assertThat(employees.markers).isEmpty();
    assertThat(employees.value).containsExactly( //
            new Employee("alice", "Alice Allison"), //
            new Employee("bob", "Bob Bobberson"), //
            new Employee("eve", "Eve Evenson"));
  }

  @Test public void markedMapToList() {
    @NonNull final DimDatabase<String> db = Objects.requireNonNull(this.db);

    TestObserver<MarkedValue<String, List<Employee>>> t = new TestObserver<>();

    db.createMarkedQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES)
            .lift(MarkedQuery.mapToList(MARKED_MAPPER))
            .skip(1)
            .subscribe(t);

    db.insertMarked("marker", TABLE_EMPLOYEE, CONFLICT_NONE, employee("john", "John Johnson"));

    t.assertValue(new MarkedValue<>("marker", Arrays.asList(
            new Employee("alice", "Alice Allison"), //
            new Employee("bob", "Bob Bobberson"), //
            new Employee("eve", "Eve Evenson"), //
            new Employee("john", "John Johnson")
    )));
  }

  @Test public void emptyMarkedMapToListEmptyWhenNoRows() {
    @NonNull final DimDatabase<String> db = Objects.requireNonNull(this.db);

    @NonNull final MarkedValue<String, List<Employee>> employeesMarkedValue =
            db.createMarkedQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES + " WHERE 1=2")
                    .lift(MarkedQuery.mapToList(MARKED_MAPPER))
                    .blockingFirst();
    assertThat(employeesMarkedValue.markers).isEmpty();
    assertThat(employeesMarkedValue.value).isEmpty();
  }

  @Test public void emptyMarkedMapToSpecificListEmptyWhenNoRows() {
    @NonNull final DimDatabase<String> db = Objects.requireNonNull(this.db);

    @NonNull final MarkedValue<String, ArrayList<Employee>> employeesMarkedValue =
            db.createMarkedQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES + " WHERE 1=2")
                    .lift(MarkedQuery.mapToSpecificList(MARKED_MAPPER, ArrayList::new))
                    .blockingFirst();
    assertThat(employeesMarkedValue.markers).isEmpty();
    assertThat(employeesMarkedValue.value).isEmpty();
  }

  @Test public void markedMapToListEmptyWhenNoRows() {
    @NonNull final DimDatabase<String> db = Objects.requireNonNull(this.db);

    TestObserver<MarkedValue<String, List<Employee>>> t = new TestObserver<>();

    db.createMarkedQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES + " WHERE 1=2")
            .lift(MarkedQuery.mapToList(MARKED_MAPPER))
            .skip(1)
            .subscribe(t);

    db.insertMarked("marker", TABLE_EMPLOYEE, CONFLICT_NONE, employee("john", "John Johnson"));

    t.assertValue(new MarkedValue<>("marker", Collections.emptyList()));
  }

  @Test public void markedMapToSpecificListEmptyWhenNoRows() {
    @NonNull final DimDatabase<String> db = Objects.requireNonNull(this.db);

    TestObserver<MarkedValue<String, ArrayList<Employee>>> t = new TestObserver<>();

    db.createMarkedQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES + " WHERE 1=2")
            .lift(MarkedQuery.mapToSpecificList(MARKED_MAPPER, ArrayList::new))
            .skip(1)
            .subscribe(t);

    db.insertMarked("marker", TABLE_EMPLOYEE, CONFLICT_NONE, employee("john", "John Johnson"));

    t.assertValue(new MarkedValue<>("marker", new ArrayList<>()));
  }

  @Test public void mapToListThrowsWhenMapperReturnsNull() {
    @NonNull final DimDatabase<String> db = Objects.requireNonNull(this.db);

    @NonNull final BiFunction<Cursor, Set<String>, Employee> mapToNull = new BiFunction<Cursor, Set<String>, Employee>() {
      private int count;

      @NonNull @Override public Employee apply(@NonNull Cursor cursor, @NonNull Set<String> markers) {
        //noinspection ConstantConditions
        return null;
      }
    };

    try {
      //noinspection ResultOfMethodCallIgnored
      db.createMarkedQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES) //
              .lift(MarkedQuery.mapToList(mapToNull)) //
              .blockingFirst();
      fail();
    } catch (NullPointerException e) {
      assertThat(e).hasMessageThat().isEqualTo("QueryToList mapper returned null");
    }
  }

  @Test public void mapToSpecificListThrowsWhenMapperReturnsNull() {
    @NonNull final DimDatabase<String> db = Objects.requireNonNull(this.db);

    @NonNull final BiFunction<Cursor, Set<String>, Employee> mapToNull = new BiFunction<Cursor, Set<String>, Employee>() {
      private int count;

      @NonNull @Override public Employee apply(@NonNull Cursor cursor, @NonNull Set<String> markers) {
        //noinspection ConstantConditions
        return null;
      }
    };

    try {
      //noinspection ResultOfMethodCallIgnored
      db.createMarkedQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES) //
              .lift(MarkedQuery.mapToSpecificList(mapToNull, ArrayList::new)) //
              .blockingFirst();
      fail();
    } catch (NullPointerException e) {
      assertThat(e).hasMessageThat().isEqualTo("QueryToList mapper returned null");
    }
  }

  @Test public void mapToListIgnoresNullCursor() {
    @NonNull final MarkedQuery<String> nully = new MarkedQuery<String>(Collections.emptySet()) {
      @Nullable @Override public Cursor run() {
        return null;
      }
    };

    @NonNull final TestObserver<MarkedValue<String, List<Employee>>> subscriber = new TestObserver<>();
    Observable.just(nully)
        .lift(MarkedQuery.mapToList(MARKED_MAPPER))
        .subscribe(subscriber);

    subscriber.assertNoValues();
    subscriber.assertComplete();
  }

  @Test public void mapToSpecificListIgnoresNullCursor() {
    @NonNull final MarkedQuery<String> nully = new MarkedQuery<String>(Collections.emptySet()) {
      @Nullable @Override public Cursor run() {
        return null;
      }
    };

    @NonNull final TestObserver<MarkedValue<String, ArrayList<Employee>>> subscriber = new TestObserver<>();
    Observable.just(nully)
        .lift(MarkedQuery.mapToSpecificList(MARKED_MAPPER, ArrayList::new))
        .subscribe(subscriber);

    subscriber.assertNoValues();
    subscriber.assertComplete();
  }

  @SdkSuppress(minSdkVersion = Build.VERSION_CODES.N)
  @Test public void emptyMarkedMapToOptional() {
    @NonNull final DimDatabase<String> db = Objects.requireNonNull(this.db);

    db.createMarkedQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES + " LIMIT 1")
            .lift(MarkedQuery.mapToOptional(MARKED_MAPPER))
            .test()
            .assertValue(new MarkedValue<>(Optional.of(new Employee("alice", "Alice Allison"))));
  }

  @SdkSuppress(minSdkVersion = Build.VERSION_CODES.N)
  @Test public void markedMapToOptional() {
    @NonNull final DimDatabase<String> db = Objects.requireNonNull(this.db);

    @NonNull final TestObserver<MarkedValue<String, Optional<Employee>>> t =
        db.createMarkedQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES + " LIMIT 1")
            .lift(MarkedQuery.mapToOptional(MARKED_MAPPER))
            .skip(1)
            .test();

    db.insertMarked("marker", TABLE_EMPLOYEE, CONFLICT_NONE, employee("john", "John Johnson"));

    t.assertValue(new MarkedValue<>("marker", Optional.of(new Employee("alice", "Alice Allison"))));
  }

  @SdkSuppress(minSdkVersion = Build.VERSION_CODES.N)
  @Test public void mapToOptionalThrowsWhenMapperReturnsNull() {
    @NonNull final DimDatabase<String> db = Objects.requireNonNull(this.db);

    db.createMarkedQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES + " LIMIT 1")
        .lift(MarkedQuery.mapToOptional(new BiFunction<Cursor, Set<String>, Employee>() {
          @NonNull @Override public Employee apply(@NonNull Cursor cursor, @NonNull Set<String> markers) {
            //noinspection ConstantConditions
            return null;
          }
        }))
        .test()
        .assertError(NullPointerException.class)
        .assertErrorMessage("QueryToOne mapper returned null");
  }

  @SdkSuppress(minSdkVersion = Build.VERSION_CODES.N)
  @Test public void mapToOptionalThrowsOnMultipleRows() {
    @NonNull final DimDatabase<String> db = Objects.requireNonNull(this.db);

    db.createMarkedQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES + " LIMIT 2") //
        .lift(MarkedQuery.mapToOptional(MARKED_MAPPER))
        .test()
        .assertError(IllegalStateException.class)
        .assertErrorMessage("Cursor returned more than 1 row");
  }

  @SdkSuppress(minSdkVersion = Build.VERSION_CODES.N)
  @Test public void mapToOptionalIgnoresNullCursor() {
    @NonNull final MarkedQuery<String> nully = new MarkedQuery<String>(Collections.emptySet()) {
      @Nullable @Override public Cursor run() {
        return null;
      }
    };

    Observable.just(nully)
        .lift(MarkedQuery.mapToOptional(MARKED_MAPPER))
        .test()
        .assertValue(new MarkedValue<>(Optional.empty()));
  }
}
