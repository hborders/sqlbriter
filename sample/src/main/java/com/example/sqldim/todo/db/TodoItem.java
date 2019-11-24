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
package com.example.sqldim.todo.db;

import android.content.ContentValues;
import android.database.Cursor;
import android.os.Parcelable;

import androidx.annotation.NonNull;

import com.google.auto.value.AutoValue;

import java.util.Objects;
import com.stealthmountain.sqldim.FunctionRR;

@AutoValue
public abstract class TodoItem implements Parcelable {
  public static final String TABLE = "todo_item";

  public static final String ID = "_id";
  public static final String LIST_ID = "todo_list_id";
  public static final String DESCRIPTION = "description";
  public static final String COMPLETE = "complete";

  public abstract long id();
  public abstract long listId();
  @NonNull public abstract String description();
  public abstract boolean complete();

  public static final FunctionRR<Cursor, TodoItem> MAPPER = new FunctionRR<Cursor, TodoItem>() {
    @NonNull @Override public TodoItem applyRR(@NonNull Cursor cursor) {
      long id = Db.getLong(cursor, ID);
      long listId = Db.getLong(cursor, LIST_ID);
      @NonNull String description = Objects.requireNonNull(Db.getString(cursor, DESCRIPTION));
      boolean complete = Db.getBoolean(cursor, COMPLETE);
      return new AutoValue_TodoItem(id, listId, description, complete);
    }
  };

  public static final class Builder {
    @NonNull private final ContentValues values = new ContentValues();

    @NonNull public Builder id(long id) {
      values.put(ID, id);
      return this;
    }

    @NonNull public Builder listId(long listId) {
      values.put(LIST_ID, listId);
      return this;
    }

    @NonNull public Builder description(@NonNull String description) {
      values.put(DESCRIPTION, description);
      return this;
    }

    @NonNull public Builder complete(boolean complete) {
      values.put(COMPLETE, complete ? Db.BOOLEAN_TRUE : Db.BOOLEAN_FALSE);
      return this;
    }

    @NonNull public ContentValues build() {
      return values; // TODO defensive copy?
    }
  }
}
