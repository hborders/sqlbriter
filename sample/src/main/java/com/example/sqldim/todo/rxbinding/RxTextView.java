package com.example.sqldim.todo.rxbinding;

import android.text.Editable;
import android.text.TextWatcher;
import android.widget.TextView;

import androidx.annotation.NonNull;

import io.reactivex.rxjava3.android.MainThreadDisposable;
import io.reactivex.rxjava3.core.Observer;

// Replaced RxBinding usage until it supports RxJava3
// https://github.com/JakeWharton/RxBinding/issues/531
public final class RxTextView {
  @NonNull
  public static InitialValueObservable<CharSequence> textChanges(@NonNull TextView textView) {
    return new TextViewTextChangesObservable(textView);
  }

  private static final class TextViewTextChangesObservable extends InitialValueObservable<CharSequence> {
    @NonNull private final TextView view;

    private TextViewTextChangesObservable(@NonNull TextView view) {
      this.view = view;
    }

    @Override
    protected void subscribeListener(@NonNull Observer<? super CharSequence> observer) {
      @NonNull final Listener listener = new Listener(view, observer);
      observer.onSubscribe(listener);
      view.addTextChangedListener(listener);
    }

    @NonNull @Override
    protected CharSequence initialValue() {
      return view.getText();
    }

    private static final class Listener extends MainThreadDisposable implements TextWatcher {
      @NonNull private final TextView view;
      @NonNull private final Observer<? super CharSequence> observer;

      private Listener(@NonNull TextView view, @NonNull Observer<? super CharSequence> observer) {
        this.view = view;
        this.observer = observer;
      }

      @Override
      public void beforeTextChanged(@NonNull CharSequence s, int start, int count, int after) {
      }

      @Override
      public void onTextChanged(@NonNull CharSequence s, int start, int before, int count) {
        if (!isDisposed()) {
          observer.onNext(s);
        }
      }

      @Override
      public void afterTextChanged(@NonNull Editable s) {
      }

      @Override
      protected void onDispose() {
        view.removeTextChangedListener(this);
      }
    }
  }
}
