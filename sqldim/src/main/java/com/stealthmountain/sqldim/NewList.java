package com.stealthmountain.sqldim;

import java.util.List;

/**
 * Lambdable interface to create a new List.
 */
public interface NewList<L extends List<T>, T> {
  L newList(int initialCapacity);
}
