package com.example.sqldim.todo.rxbinding;

import android.view.View;
import android.widget.AdapterView;

import androidx.annotation.NonNull;
import androidx.annotation.Nullable;

// Replaced RxBinding usage until it supports RxJava3
// https://github.com/JakeWharton/RxBinding/issues/531
public final class AdapterViewItemClickEvent {
  @NonNull private final AdapterView<?> _parent;
  @Nullable private final View _view;
  private final int _position;
  private final long _id;

  public AdapterViewItemClickEvent(@NonNull AdapterView<?> parent, @Nullable View view,
                                   int position, long id) {
    _parent = parent;
    _view = view;
    _position = position;
    _id = id;
  }

  @NonNull
  public final AdapterView<?> parent() {
    return _parent;
  }

  @Nullable
  public final View view() {
    return _view;
  }

  public final int position() {
    return _position;
  }

  public final long id() {
    return _id;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    AdapterViewItemClickEvent that = (AdapterViewItemClickEvent) o;

    if (_position != that._position) return false;
    if (_id != that._id) return false;
    if (!_parent.equals(that._parent)) return false;
    return _view != null ? _view.equals(that._view) : that._view == null;
  }

  @Override
  public int hashCode() {
    int result = _parent.hashCode();
    result = 31 * result + (_view != null ? _view.hashCode() : 0);
    result = 31 * result + _position;
    result = 31 * result + (int) (_id ^ (_id >>> 32));
    return result;
  }

  @Override
  public String toString() {
    return "AdapterViewItemClickEvent{" +
           "parent=" + _parent +
           ", view=" + _view +
           ", position=" + _position +
           ", id=" + _id +
           '}';
  }
}
