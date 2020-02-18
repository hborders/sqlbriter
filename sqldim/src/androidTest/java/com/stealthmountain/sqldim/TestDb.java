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

import androidx.annotation.Nullable;
import androidx.sqlite.db.SupportSQLiteDatabase;
import androidx.sqlite.db.SupportSQLiteOpenHelper;
import android.content.ContentValues;
import android.database.Cursor;
import androidx.annotation.NonNull;

import io.reactivex.rxjava3.functions.BiFunction;
import io.reactivex.rxjava3.functions.Function;

import java.util.Arrays;
import java.util.Collection;
import java.util.Set;

import static android.database.sqlite.SQLiteDatabase.CONFLICT_FAIL;
import static com.stealthmountain.sqldim.TestDb.EmployeeTable.ID;
import static com.stealthmountain.sqldim.TestDb.EmployeeTable.NAME;
import static com.stealthmountain.sqldim.TestDb.EmployeeTable.USERNAME;
import static com.stealthmountain.sqldim.TestDb.ManagerTable.EMPLOYEE_ID;
import static com.stealthmountain.sqldim.TestDb.ManagerTable.MANAGER_ID;

final class TestDb extends SupportSQLiteOpenHelper.Callback {
  @NonNull static final String TABLE_EMPLOYEE = "employee";
  @NonNull static final String TABLE_MANAGER = "manager";

  @NonNull static final String SELECT_EMPLOYEES =
      "SELECT " + USERNAME + ", " + NAME + " FROM " + TABLE_EMPLOYEE;
  @NonNull static final String SELECT_MANAGER_LIST = ""
      + "SELECT e." + NAME + ", m." + NAME + " "
      + "FROM " + TABLE_MANAGER + " AS manager "
      + "JOIN " + TABLE_EMPLOYEE + " AS e "
      + "ON manager." + EMPLOYEE_ID + " = e." + ID + " "
      + "JOIN " + TABLE_EMPLOYEE + " as m "
      + "ON manager." + MANAGER_ID + " = m." + ID;
  @NonNull static final Collection<String> BOTH_TABLES =
      Arrays.asList(TABLE_EMPLOYEE, TABLE_MANAGER);

  interface EmployeeTable {
    @NonNull String ID = "_id";
    @NonNull String USERNAME = "username";
    @NonNull String NAME = "name";
  }

  static final class Employee {
    @NonNull static final Function<Cursor, Employee> MAPPER = new Function<Cursor, Employee>() {
      @NonNull @Override public Employee apply(@NonNull Cursor cursor) {
        return new Employee( //
                cursor.getString(cursor.getColumnIndexOrThrow(EmployeeTable.USERNAME)),
                cursor.getString(cursor.getColumnIndexOrThrow(EmployeeTable.NAME)));
      }
    };
    @NonNull static final BiFunction<Cursor, Set<String>, Employee> MARKED_MAPPER = new BiFunction<Cursor, Set<String>, Employee>() {
      @NonNull @Override public Employee apply(@NonNull Cursor cursor, @NonNull Set<String> markers) {
        return new Employee( //
                cursor.getString(cursor.getColumnIndexOrThrow(EmployeeTable.USERNAME)),
                cursor.getString(cursor.getColumnIndexOrThrow(EmployeeTable.NAME)));
      }
    };


    @Nullable final String username;
    @Nullable final String name;


    Employee(@Nullable String username, @Nullable String name) {
      this.username = username;
      this.name = name;
    }

    @Override public boolean equals(@Nullable Object o) {
      if (o == this) return true;
      if (!(o instanceof Employee)) return false;
      @NonNull final Employee other = (Employee) o;
      return ((username != null && username.equals(other.username)) || (username == null && other.username == null)) &&
              ((name != null && name.equals(other.name)) || (name == null && other.name == null));
    }

    @Override public int hashCode() {
      return (username == null ? 0 : username.hashCode()) +
              (name == null ? 0 : name.hashCode()) * 17;
    }

    @Override public String toString() {
      return "Employee[" + username + ' ' + name + ']';
    }
  }

  interface ManagerTable {
    @NonNull String ID = "_id";
    @NonNull String EMPLOYEE_ID = "employee_id";
    @NonNull String MANAGER_ID = "manager_id";
  }

  @NonNull private static final String CREATE_EMPLOYEE = "CREATE TABLE " + TABLE_EMPLOYEE + " ("
      + EmployeeTable.ID + " INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, "
      + EmployeeTable.USERNAME + " TEXT NOT NULL UNIQUE, "
      + EmployeeTable.NAME + " TEXT NOT NULL)";
  @NonNull private static final String CREATE_MANAGER = "CREATE TABLE " + TABLE_MANAGER + " ("
      + ManagerTable.ID + " INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, "
      + ManagerTable.EMPLOYEE_ID + " INTEGER NOT NULL UNIQUE REFERENCES " + TABLE_EMPLOYEE + "(" + EmployeeTable.ID + "), "
      + ManagerTable.MANAGER_ID + " INTEGER NOT NULL REFERENCES " + TABLE_EMPLOYEE + "(" + EmployeeTable.ID + "))";

  long aliceId;
  long bobId;
  long eveId;

  TestDb() {
    super(1);
  }

  @Override public void onCreate(@NonNull SupportSQLiteDatabase db) {
    db.execSQL("PRAGMA foreign_keys=ON");

    db.execSQL(CREATE_EMPLOYEE);
    aliceId = db.insert(TABLE_EMPLOYEE, CONFLICT_FAIL, employee("alice", "Alice Allison"));
    bobId = db.insert(TABLE_EMPLOYEE, CONFLICT_FAIL, employee("bob", "Bob Bobberson"));
    eveId = db.insert(TABLE_EMPLOYEE, CONFLICT_FAIL, employee("eve", "Eve Evenson"));

    db.execSQL(CREATE_MANAGER);
    db.insert(TABLE_MANAGER, CONFLICT_FAIL, manager(eveId, aliceId));
  }

  @NonNull static ContentValues employee(@NonNull String username, @NonNull String name) {
    @NonNull final ContentValues values = new ContentValues();
    values.put(EmployeeTable.USERNAME, username);
    values.put(EmployeeTable.NAME, name);
    return values;
  }

  @NonNull static ContentValues manager(long employeeId, long managerId) {
    @NonNull final ContentValues values = new ContentValues();
    values.put(ManagerTable.EMPLOYEE_ID, employeeId);
    values.put(ManagerTable.MANAGER_ID, managerId);
    return values;
  }

  @Override
  public void onUpgrade(@NonNull SupportSQLiteDatabase db, int oldVersion, int newVersion) {
    throw new AssertionError();
  }
}
