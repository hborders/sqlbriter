package com.stealthmountain.sqldim;

import android.database.Cursor;
import android.database.MatrixCursor;

import androidx.annotation.NonNull;
import androidx.annotation.Nullable;
import androidx.test.ext.junit.runners.AndroidJUnit4;
import com.stealthmountain.sqldim.SqlBrite.Query;
import com.stealthmountain.sqldim.SqlBrite.MarkedQuery;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;
import org.junit.runner.RunWith;

import io.reactivex.functions.BiFunction;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.fail;

@RunWith(AndroidJUnit4.class)
@SuppressWarnings("CheckResult")
public final class SqlDimTest {
  @NonNull private static final String FIRST_NAME = "first_name";
  @NonNull private static final String LAST_NAME = "last_name";
  @NonNull private static final String[] COLUMN_NAMES = { FIRST_NAME, LAST_NAME };

  @Test public void builderDisallowsNull() {
    @NonNull final SqlBrite.Builder<Object> builder = new SqlBrite.Builder<>();
    try {
      //noinspection ConstantConditions
      builder.logger(null);
      fail();
    } catch (NullPointerException e) {
      assertThat(e).hasMessageThat().isEqualTo("logger == null");
    }
    try {
      //noinspection ConstantConditions
      builder.queryTransformer(null);
      fail();
    } catch (NullPointerException e) {
      assertThat(e).hasMessageThat().isEqualTo("queryTransformer == null");
    }
    try {
      //noinspection ConstantConditions
      builder.markedQueryTransformer(null);
      fail();
    } catch (NullPointerException e) {
      assertThat(e).hasMessageThat().isEqualTo("markedQueryTransformer == null");
    }
  }

  @Test public void asRowsEmpty() {
    @NonNull final MatrixCursor cursor = new MatrixCursor(COLUMN_NAMES);
    @NonNull final Query query = new CursorQuery(cursor);
    @NonNull final List<Name> names = query.asRows(Name.MAP).toList().blockingGet();
    assertThat(names).isEmpty();
  }

  @Test public void markedQueryAsRowsEmpty() {
    @NonNull final MatrixCursor cursor = new MatrixCursor(COLUMN_NAMES);
    @NonNull final MarkedQuery<String> markedQuery = new CursorMarkedQuery(cursor);
    @NonNull final List<Name> names = markedQuery.asRows(Name.MARKED_MAP).toList().blockingGet();
    assertThat(names).isEmpty();
  }

  @Test public void asRows() {
    @NonNull final MatrixCursor cursor = new MatrixCursor(COLUMN_NAMES);
    cursor.addRow(new Object[] { "Alice", "Allison" });
    cursor.addRow(new Object[] { "Bob", "Bobberson" });

    @NonNull final Query query = new CursorQuery(cursor);
    @NonNull final List<Name> names = query.asRows(Name.MAP).toList().blockingGet();
    assertThat(names).containsExactly(new Name("Alice", "Allison"), new Name("Bob", "Bobberson"));
  }

  @Test public void markedQueryAsRows() {
    @NonNull final MatrixCursor cursor = new MatrixCursor(COLUMN_NAMES);
    cursor.addRow(new Object[] { "Alice", "Allison" });
    cursor.addRow(new Object[] { "Bob", "Bobberson" });

    @NonNull final MarkedQuery<String> markedQuery = new CursorMarkedQuery(cursor);
    @NonNull final List<Name> names = markedQuery.asRows(Name.MARKED_MAP).toList().blockingGet();
    assertThat(names).containsExactly(new Name("Alice", "Allison"), new Name("Bob", "Bobberson"));
  }

  @Test public void asRowsStopsWhenUnsubscribed() {
    @NonNull final MatrixCursor cursor = new MatrixCursor(COLUMN_NAMES);
    cursor.addRow(new Object[] { "Alice", "Allison" });
    cursor.addRow(new Object[] { "Bob", "Bobberson" });

    @NonNull final Query query = new CursorQuery(cursor);
    @NonNull final AtomicInteger count = new AtomicInteger();
    //noinspection ResultOfMethodCallIgnored
    query.asRows(new FunctionRR<Cursor, Name>() {
      @NonNull @Override public Name applyRR(@NonNull Cursor cursor) throws Exception {
        count.incrementAndGet();
        return Name.MAP.applyRR(cursor);
      }
    }).take(1).blockingFirst();
    assertThat(count.get()).isEqualTo(1);
  }

  @Test public void markedQueryAsRowsStopsWhenUnsubscribed() {
    @NonNull final MatrixCursor cursor = new MatrixCursor(COLUMN_NAMES);
    cursor.addRow(new Object[] { "Alice", "Allison" });
    cursor.addRow(new Object[] { "Bob", "Bobberson" });

    @NonNull final MarkedQuery<String> markedQuery = new CursorMarkedQuery(cursor);
    @NonNull final AtomicInteger count = new AtomicInteger();
    //noinspection ResultOfMethodCallIgnored
    markedQuery.asRows(new BiFunction<Cursor, Set<String>, Name>() {
      @NonNull @Override public Name apply(@NonNull Cursor cursor, @NonNull Set<String> markers) throws Exception {
        count.incrementAndGet();
        return Name.MARKED_MAP.apply(cursor, markers);
      }
    }).take(1).blockingFirst();
    assertThat(count.get()).isEqualTo(1);
  }

  @Test public void asRowsEmptyWhenNullCursor() {
    @NonNull final Query nully = new Query() {
      @Nullable @Override public Cursor run() {
        return null;
      }
    };

    @NonNull final AtomicInteger count = new AtomicInteger();
    nully.asRows(new FunctionRR<Cursor, Name>() {
      @NonNull @Override public Name applyRR(@NonNull Cursor cursor) throws Exception {
        count.incrementAndGet();
        return Name.MAP.applyRR(cursor);
      }
    }).test().assertNoValues().assertComplete();

    assertThat(count.get()).isEqualTo(0);
  }

  @Test public void markedQueryAsRowsEmptyWhenNullCursor() {
    @NonNull final MarkedQuery<String> nully = new MarkedQuery<String>(Collections.emptySet()) {
      @Nullable @Override public Cursor run() {
        return null;
      }
    };

    @NonNull final AtomicInteger count = new AtomicInteger();
    nully.asRows(new BiFunction<Cursor, Set<String>, Name>() {
      @NonNull @Override public Name apply(@NonNull Cursor cursor, @NonNull Set<String> markers) throws Exception {
        count.incrementAndGet();
        return Name.MARKED_MAP.apply(cursor, markers);
      }
    }).test().assertNoValues().assertComplete();

    assertThat(count.get()).isEqualTo(0);
  }

  static final class Name {
    @NonNull static final FunctionRR<Cursor, Name> MAP = new FunctionRR<Cursor, Name>() {
      @NonNull @Override public Name applyRR(@NonNull Cursor cursor) {
        return new Name( //
                cursor.getString(cursor.getColumnIndexOrThrow(FIRST_NAME)),
                cursor.getString(cursor.getColumnIndexOrThrow(LAST_NAME)));
      }
    };

    @NonNull static final BiFunction<Cursor, Set<String>, Name> MARKED_MAP = new BiFunction<Cursor, Set<String>, Name>() {
      @NonNull @Override public Name apply(@NonNull Cursor cursor, @NonNull Set<String> markers) {
        return new Name( //
                cursor.getString(cursor.getColumnIndexOrThrow(FIRST_NAME)),
                cursor.getString(cursor.getColumnIndexOrThrow(LAST_NAME)));
      }
    };

    @Nullable final String first;
    @Nullable final String last;

    Name(@Nullable String first, @Nullable String last) {
      this.first = first;
      this.last = last;
    }

    @Override public boolean equals(Object o) {
      if (o == this) return true;
      if (!(o instanceof Name)) return false;
      @NonNull final Name other = (Name) o;

      return ((first != null && first.equals(other.first)) || (first == null && other.first == null)) &&
              ((last != null && last.equals(other.last)) || (last == null && other.last == null));
    }

    @Override public int hashCode() {
      return (first == null ? 0 : first.hashCode() * 17) + (last == null ? 0 : last.hashCode());
    }

    @Override public String toString() {
      return "Name[" + first + ' ' + last + ']';
    }
  }

  static final class CursorQuery extends Query {
    @NonNull private final Cursor cursor;

    CursorQuery(@NonNull Cursor cursor) {
      this.cursor = cursor;
    }

    @NonNull @Override public Cursor run() {
      return cursor;
    }
  }

  static final class CursorMarkedQuery extends MarkedQuery<String> {
    @NonNull private final Cursor cursor;
    CursorMarkedQuery(@NonNull Cursor cursor) {
      super(Collections.emptySet());
      this.cursor = cursor;
    }

    @NonNull @Override public Cursor run() {
      return cursor;
    }
  }
}
