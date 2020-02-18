package com.example.sqldim.todo.rxbinding;

import android.os.Looper;
import android.view.View;
import android.widget.Adapter;
import android.widget.AdapterView;

import androidx.annotation.NonNull;
import androidx.annotation.Nullable;

import io.reactivex.rxjava3.android.MainThreadDisposable;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Observer;
import io.reactivex.rxjava3.disposables.Disposable;

// Replaced RxBinding usage until it supports RxJava3
// https://github.com/JakeWharton/RxBinding/issues/531
public final class RxAdapterView {
  @NonNull
  public static <T extends Adapter> Observable<AdapterViewItemClickEvent> itemClickEvents(@NonNull AdapterView<T> adapterView) {
    return new AdapterViewClickEventObservable(adapterView);
  }

  private static final class AdapterViewClickEventObservable extends Observable<AdapterViewItemClickEvent> {
    @NonNull
    private final AdapterView<?> view;

    private AdapterViewClickEventObservable(@NonNull AdapterView<?> view) {
      this.view = view;
    }

    @Override
    protected void subscribeActual(@NonNull Observer<? super AdapterViewItemClickEvent> observer) {
      if (!checkMainThread(observer)) {
        return;
      }

      @NonNull final Listener listener = new Listener(view, observer);
      observer.onSubscribe(listener);
      view.setOnItemClickListener(listener);
    }

    private boolean checkMainThread(@NonNull Observer<?> observer) {
      if (Looper.myLooper() != Looper.getMainLooper()) {
        observer.onSubscribe(Disposable.empty());
        observer.onError(new IllegalStateException(
                "Expected to be called on the main thread but was " + Thread.currentThread().getName()));
        return false;
      } else {
        return true;
      }
    }

    private static final class Listener extends MainThreadDisposable implements AdapterView.OnItemClickListener {
      @NonNull private final AdapterView<?> view;
      @NonNull private final Observer<? super AdapterViewItemClickEvent> observer;

      private Listener(@NonNull AdapterView<?> view, @NonNull Observer<? super AdapterViewItemClickEvent> observer) {
        this.view = view;
        this.observer = observer;
      }

      @Override
      public void onItemClick(@NonNull AdapterView<?> parent, @Nullable View view, int position, long id) {
        observer.onNext(new AdapterViewItemClickEvent(parent, view, position, id));
      }

      @Override
      protected void onDispose() {
        view.setOnItemClickListener(null);
      }
    }
  }
}
