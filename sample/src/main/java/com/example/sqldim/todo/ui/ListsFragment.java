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
package com.example.sqldim.todo.ui;

import android.content.Context;
import android.os.Bundle;
import androidx.annotation.Nullable;
import androidx.annotation.NonNull;
import androidx.fragment.app.Fragment;
import androidx.core.view.MenuItemCompat;
import android.view.LayoutInflater;
import android.view.Menu;
import android.view.MenuInflater;
import android.view.MenuItem;
import android.view.View;
import android.view.ViewGroup;
import android.widget.AdapterView;
import android.widget.ListView;
import com.example.sqldim.todo.R;
import com.example.sqldim.todo.TodoApp;
import com.stealthmountain.sqldim.DimDatabase;

import javax.inject.Inject;

import io.reactivex.rxjava3.android.schedulers.AndroidSchedulers;
import io.reactivex.rxjava3.disposables.Disposable;

import static androidx.core.view.MenuItemCompat.SHOW_AS_ACTION_IF_ROOM;
import static androidx.core.view.MenuItemCompat.SHOW_AS_ACTION_WITH_TEXT;

public final class ListsFragment extends Fragment {
  interface Listener {
    void onListClicked(long id);
    void onNewListClicked();
  }

  static ListsFragment newInstance() {
    return new ListsFragment();
  }

  @Inject
  DimDatabase<Object> db;

  private Listener listener;
  private ListsAdapter adapter;
  private Disposable disposable;

  @Override public void onAttach(@NonNull Context context) {
    if (!(context instanceof Listener)) {
      throw new IllegalStateException("Activity must implement fragment Listener.");
    }

    super.onAttach(context);
    TodoApp.getComponent(context).inject(this);
    setHasOptionsMenu(true);

    listener = (Listener) context;
    adapter = new ListsAdapter(context);
  }

  @Override public void onCreateOptionsMenu(@NonNull Menu menu, @NonNull MenuInflater inflater) {
    super.onCreateOptionsMenu(menu, inflater);

    MenuItem item = menu.add(R.string.new_list)
        .setOnMenuItemClickListener(new MenuItem.OnMenuItemClickListener() {
          @Override public boolean onMenuItemClick(MenuItem item) {
            listener.onNewListClicked();
            return true;
          }
        });
    MenuItemCompat.setShowAsAction(item, SHOW_AS_ACTION_IF_ROOM | SHOW_AS_ACTION_WITH_TEXT);
  }

  @Override public View onCreateView(LayoutInflater inflater, @Nullable ViewGroup container,
      @Nullable Bundle savedInstanceState) {
    return inflater.inflate(R.layout.lists, container, false);
  }

  @Override
  public void onViewCreated(@NonNull View view, @Nullable Bundle savedInstanceState) {
    super.onViewCreated(view, savedInstanceState);
    ListView listView = view.findViewById(android.R.id.list);
    View emptyView = view.findViewById(android.R.id.empty);
    listView.setEmptyView(emptyView);
    listView.setAdapter(adapter);
    listView.setOnItemClickListener(new AdapterView.OnItemClickListener() {
      @Override
      public void onItemClick(AdapterView<?> parent, View view, int position, long id) {
        listener.onListClicked(id);
      }
    });
  }

  @Override public void onResume() {
    super.onResume();

    getActivity().setTitle("To-Do");

    disposable = db.createQuery(ListsItem.TABLES, ListsItem.QUERY)
        .mapToList(ListsItem.MAPPER)
        .observeOn(AndroidSchedulers.mainThread())
        .subscribe(adapter);
  }

  @Override public void onPause() {
    super.onPause();
    disposable.dispose();
  }
}
