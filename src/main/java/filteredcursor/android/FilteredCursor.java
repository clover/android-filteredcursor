/*
 * Copyright (C) 2014 Clover Network, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package filteredcursor.android;

import android.database.CharArrayBuffer;
import android.database.Cursor;
import android.database.CursorIndexOutOfBoundsException;
import android.database.CursorWrapper;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;

/**
 * Wraps a Cursor and allow its positions to be filtered out, repeated, or reordered.
 *
 * Note that if the source Cursor exceeds the size of the CursorWindow (2MB) the FilteredCursor may end up with
 * extremely poor performance due to frequent CursorWindow cache misses. In those cases it is recommended that source
 * Cursor contain less data.
 *
 * @author Jacob Whitaker Abrams
 */
public class FilteredCursor extends CursorWrapper {

  // Globally map master Cursor to FilteredCursors, when all FilteredCursors are closed go ahead and close the master
  // This would need to go into a singleton if other classes similar to FilteredCursor exist
  private static final Map<Cursor, Set<FilteredCursor>> sMasterCursorMap =
      Collections.synchronizedMap(new WeakHashMap<Cursor, Set<FilteredCursor>>());

  private int[] mFilterMap;
  private int mPos = -1;
  private final Cursor mCursor;
  private boolean mClosed;

  /**
   * An interface to select rows to appear in a new FilteredCursor.
   */
  public interface Selector {
    /**
     * Return true to include the current position of the source Cursor into a FilteredCursor.
     */
    public boolean select(Cursor cursor);
  }

  /**
   * Create a FilteredCursor that contains only rows selected by the given Selector. The order of the rows in the
   * FilteredCursor matches the order of the source Cursor.
   */
  public static FilteredCursor createUsingSelector(Cursor cursor, Selector selector) {
    if (cursor == null) {
      return null;
    }

    ArrayList<Integer> filterList = new ArrayList<Integer>();

    if (cursor.moveToFirst()) {
      do {
        if (selector.select(cursor)) {
          filterList.add(cursor.getPosition());
        }
      } while (cursor.moveToNext());
    }

    return new FilteredCursor(cursor, toIntArray(filterList));
  }

  /**
   * When a source Cursor is joined with a list of values the JoinType specifies how rows are filtered. The JoinTypes
   * operates similarly to standard SQL joins.
   */
  public enum JoinType {
    /**
     * Specifies a left outer join, for rows where there is no match in the cursor {@link FilteredCursor#isEmpty()}
     * returns true.
     */
    LEFT_OUTER_JOIN,
    /**
     * Just like {@link JoinType#LEFT_OUTER_JOIN} except every row must match, if not {@link IllegalArgumentException}
     * is thrown.
     */
    STRICT_LEFT_OUTER_JOIN,
    /** Specifies an inner join. */
    INNER_JOIN,
  }

  /**
   * Create a new FilteredCursor using a {@link JoinType#STRICT_LEFT_OUTER_JOIN}. The joinList acts as the left table
   * with a single column of values and source Cursor acts as the right table. The join is performed on the values of
   * the given columnName. Returns null if the provided cursor
   * is null.
   *
   * @see JoinType
   */
  public static FilteredCursor createUsingJoinList(Cursor cursor, String columnName, List<String> joinList) {
    return createUsingJoinList(cursor, columnName, joinList, JoinType.STRICT_LEFT_OUTER_JOIN);
  }

  /**
   * Create a new FilteredCursor using a {@link JoinType#STRICT_LEFT_OUTER_JOIN}. The joinList acts as the left table
   * with a single column of values and source Cursor acts as the right table. The join is performed on the values of
   * the given columnName. Returns null if the provided cursor
   * is null.
   *
   * @see JoinType
   */
  public static FilteredCursor createUsingJoinList(Cursor cursor, int columnIndex, List<String> joinList) {
    return createUsingJoinList(cursor, columnIndex, joinList, JoinType.STRICT_LEFT_OUTER_JOIN);
  }

  /**
   * Create a new FilteredCursor by joining the source Cursor with the given list using a {@link JoinType}. The
   * joinList acts as the left table with a single column of values and source Cursor acts as the right table. The join
   * is performed on the values of the given columnName. Returns null if the provided cursor is null.
   *
   * @see JoinType
   */
  public static FilteredCursor createUsingJoinList(Cursor cursor, String columnName, List<String> joinList, JoinType joinType) {
    if (cursor == null) {
      return null;
    }
    final int columnIndex = cursor.getColumnIndexOrThrow(columnName);
    return new FilteredCursor(cursor, columnIndex, joinList, joinType);
  }

  /**
   * Just like {@link FilteredCursor#createUsingJoinList(Cursor, String, List<String>, JoinType)}, except it takes a
   * column index instead of column name.
   *
   * @see JoinType
   */
  public static FilteredCursor createUsingJoinList(Cursor cursor, int columnIndex, List<String> joinList, JoinType joinType) {
    if (cursor == null) {
      return null;
    }
    return new FilteredCursor(cursor, columnIndex, joinList, joinType);
  }

  /**
   * Create a FilteredCursor that appears identical to its wrapped Cursor.
   */
  public static FilteredCursor createIdentityFilteredCursor(Cursor cursor) {
    if (cursor == null) {
      return null;
    }
    return new FilteredCursor(cursor);
  }

  /**
   * Create a new FilteredCursor using the given filter. The filterMap specifies where rows of the given Cursor should
   * appear in the FilteredCursor. For example if filterMap = { 5, 9 } then the FilteredCursor will have two rows, the
   * first row maps to row 5 in the source Cursor and the second row maps to row 9 in the source cursor. Returns null
   * if the provided cursor is null.
   * */
  public static FilteredCursor createUsingFilter(Cursor cursor, int[] filter) {
    if (cursor == null) {
      return null;
    }
    if (filter == null) {
      throw new NullPointerException();
    }
    return new FilteredCursor(cursor, filter);
  }

  /**
   * Create a group of FilteredCursors. Each FilteredCursor will contain only rows with the same value for the given
   * column. The order of the rows in each FilteredCursor matches the order of the source Cursor. NULL is not treated
   * as unique so if there are any rows with the value NULL they will be grouped together.
   */
  public static Map<String, FilteredCursor> createGroups(Cursor cursor, String columnName) {
    if (cursor == null) {
      return null;
    }
    final int columnIndex = cursor.getColumnIndexOrThrow(columnName);
    return createGroups(cursor, columnIndex);
  }

  /**
   * Just like {@link FilteredCursor#createGroups(Cursor, String)}, except takes a column index instead of column name.
   */
  public static Map<String, FilteredCursor> createGroups(Cursor cursor, int columnIndex) {
    if (cursor == null) {
      return null;
    }

    Map<String, List<Integer>> filters = new HashMap<String, List<Integer>>();

    if (cursor.moveToFirst()) {
      do {
        String key = cursor.getString(columnIndex);
        List<Integer> filterList = filters.get(key);
        if (filterList == null) {
          filterList = new ArrayList<Integer>();
          filters.put(key, filterList);
        }
        filterList.add(cursor.getPosition());
      } while (cursor.moveToNext());
    }

    Map<String, FilteredCursor> groups = new HashMap<String, FilteredCursor>();

    for (Map.Entry<String, List<Integer>> entry : filters.entrySet()) {
      groups.put(entry.getKey(), new FilteredCursor(cursor, toIntArray(entry.getValue())));
    }

    return groups;
  }

  private FilteredCursor(Cursor cursor) {
    this(cursor, null);
    resetToIdentityFilter();
  }

  private FilteredCursor(Cursor cursor, int[] filterMap) {
    super(cursor);

    mCursor = cursor;
    mFilterMap = filterMap;

    attachToMasterCursor();
  }

  private FilteredCursor(Cursor cursor, int columnIndex, List<String> joinList, JoinType joinType) {
    this(cursor, null);

    if (joinList == null || joinList.size() == 0) {
      mFilterMap = new int[0];
      return;
    }

    final int filterListSize = joinList.size();
    mFilterMap = new int[filterListSize];
    // -1 is a magic value indicating this position has not been mapped, which is illegal
    Arrays.fill(mFilterMap, -1);

    Map<String, Deque<Integer>> filterValueMap = new HashMap<String, Deque<Integer>>(filterListSize);

    for (int i = 0; i < filterListSize; i++) {
      String value = joinList.get(i);

      Deque<Integer> filterIndexList = filterValueMap.get(value);
      if (filterIndexList == null) {
        filterIndexList = new ArrayDeque<Integer>();
        filterValueMap.put(value, filterIndexList);
      }
      filterIndexList.add(i);
    }

    if (cursor.moveToFirst()) {
      do {
        String value = cursor.getString(columnIndex);
        Deque<Integer> filterIndexList = filterValueMap.get(value);

        if (filterIndexList != null) {
          int cursorPosition = cursor.getPosition();

          for (Integer filterIndex : filterIndexList) {
            mFilterMap[filterIndex] = cursorPosition;
          }

          // If this cursor value comes up again point remaining filter indexes to it
          if (filterIndexList.size() > 1) {
            filterIndexList.removeFirst();
          } else {
            filterValueMap.remove(value);
          }
        }
      } while (cursor.moveToNext());
    }

    switch (joinType) {
      case STRICT_LEFT_OUTER_JOIN: {
        failOnEmptyPositions(columnIndex, filterValueMap);
        break;
      }
      case INNER_JOIN: {
        cullEmptyPositions();
        break;
      }
      case LEFT_OUTER_JOIN: {
        // no need to do anything
        break;
      }
    }
  }

  private void failOnEmptyPositions(int columnIndex, Map<String, Deque<Integer>> filterValueMap) {
    for (Map.Entry<String, Deque<Integer>> filterMapEntry : filterValueMap.entrySet()) {
      int filterIndex = filterMapEntry.getValue().getFirst();
      if (mFilterMap[filterIndex] == -1) {
        throw new IllegalArgumentException("Source cursor is missing entries for the column \""
            + getColumnName(columnIndex) + "\" with values " + filterMapEntry.getKey());
      }
    }
  }

  private void cullEmptyPositions() {
    int culledSize = 0;
    for (int value : mFilterMap) {
      if (value != -1) {
        culledSize++;
      }
    }
    int[] culledFilterMap = new int[culledSize];
    int pos = 0;
    for (int value : mFilterMap) {
      if (value != -1) {
        culledFilterMap[pos++] = value;
      }
    }
    mFilterMap = culledFilterMap;
  }

  private static int[] toIntArray(List<Integer> list)  {
    int[] ret = new int[list.size()];
    int i = 0;
    for (Integer e : list)
      ret[i++] = e;
    return ret;
  }

  public int[] getFilterMap() {
    return mFilterMap;
  }

  /**
   * Reset the filter so it appears identical to its wrapped Cursor.
   */
  public FilteredCursor resetToIdentityFilter() {
    int count = mCursor.getCount();
    int[] filterMap = new int[count];

    for (int i = 0; i < count; i++) {
      filterMap[i] = i;
    }

    mFilterMap = filterMap;
    mPos = -1;
    return this;
  }

  /**
   * Returns true if the FilteredCursor appears identical to its wrapped Cursor.
   */
  public boolean isIdentityFilter() {
    int count = mCursor.getCount();
    if (mFilterMap.length != count) {
      return false;
    }

    for (int i = 0; i < count; i++) {
      if (mFilterMap[i] != i) {
        return false;
      }
    }

    return true;
  }

  /**
   * Rearrange the filter. The new arrangement is based on the current filter arrangement, not on the original wrapped
   * Cursor's arrangement.
   */
  public FilteredCursor refilter(int[] newArrangement) {
    final int newMapSize = newArrangement.length;
    int[] newMap = new int[newMapSize];
    for (int i = 0; i < newMapSize; i++) {
      newMap[i] = mFilterMap[newArrangement[i]];
    }

    mFilterMap = newMap;
    mPos = -1;
    return this;
  }

  /**
   * True if the current cursor position has no data. Attempting to access data in an empty row with any of the getters
   * will throw {@link CursorIndexOutOfBoundsException}.
   */
  public boolean isEmpty() {
    return mFilterMap[mPos] == -1;
  }

  private void throwIfEmptyRow() {
    if (isEmpty()) {
      throw new CursorIndexOutOfBoundsException("Cannot access data in an empty row");
    }
  }

  public void swapItems(int itemOne, int itemTwo) {
    int temp = mFilterMap[itemOne];
    mFilterMap[itemOne] = mFilterMap[itemTwo];
    mFilterMap[itemTwo] = temp;
  }

  @Override
  public int getCount() {
    return mFilterMap.length;
  }

  @Override
  public int getPosition() {
    return mPos;
  }

  @Override
  public boolean moveToPosition(int position) {
    // Make sure position isn't past the end of the cursor
    final int count = getCount();
    if (position >= count) {
      mPos = count;
      return false;
    }

    // Make sure position isn't before the beginning of the cursor
    if (position < 0) {
      mPos = -1;
      return false;
    }

    final int realPosition = mFilterMap[position];

    // When moving to an empty position, just pretend we did it
    boolean moved = realPosition == -1 ? true : super.moveToPosition(realPosition);
    if (moved) {
      mPos = position;
    } else {
      mPos = -1;
    }
    return moved;
  }

  @Override
  public final boolean move(int offset) {
    return moveToPosition(mPos + offset);
  }

  @Override
  public final boolean moveToFirst() {
    return moveToPosition(0);
  }

  @Override
  public final boolean moveToLast() {
    return moveToPosition(getCount() - 1);
  }

  @Override
  public final boolean moveToNext() {
    return moveToPosition(mPos + 1);
  }

  @Override
  public final boolean moveToPrevious() {
    return moveToPosition(mPos - 1);
  }

  @Override
  public final boolean isFirst() {
    return mPos == 0 && getCount() != 0;
  }

  @Override
  public final boolean isLast() {
    int cnt = getCount();
    return mPos == (cnt - 1) && cnt != 0;
  }

  @Override
  public final boolean isBeforeFirst() {
    if (getCount() == 0) {
      return true;
    }
    return mPos == -1;
  }

  @Override
  public final boolean isAfterLast() {
    if (getCount() == 0) {
      return true;
    }
    return mPos == getCount();
  }

  @Override
  public boolean isNull(int columnIndex) {
    throwIfEmptyRow();
    return mCursor.isNull(columnIndex);
  }

  @Override
  public void copyStringToBuffer(int columnIndex, CharArrayBuffer buffer) {
    throwIfEmptyRow();
    mCursor.copyStringToBuffer(columnIndex, buffer);
  }

  @Override
  public byte[] getBlob(int columnIndex) {
    throwIfEmptyRow();
    return mCursor.getBlob(columnIndex);
  }

  @Override
  public double getDouble(int columnIndex) {
    throwIfEmptyRow();
    return mCursor.getDouble(columnIndex);
  }

  @Override
  public float getFloat(int columnIndex) {
    throwIfEmptyRow();
    return mCursor.getFloat(columnIndex);
  }

  @Override
  public int getInt(int columnIndex) {
    throwIfEmptyRow();
    return mCursor.getInt(columnIndex);
  }

  @Override
  public long getLong(int columnIndex) {
    throwIfEmptyRow();
    return mCursor.getLong(columnIndex);
  }

  @Override
  public short getShort(int columnIndex) {
    throwIfEmptyRow();
    return mCursor.getShort(columnIndex);
  }

  @Override
  public String getString(int columnIndex) {
    throwIfEmptyRow();
    return mCursor.getString(columnIndex);
  }

  @Override
  public boolean isClosed() {
    return mClosed || getMasterCursor().isClosed();
  }

  @Override
  public void close() {
    // Mark this Cursor as closed
    mClosed = true;

    // Find the master Cursor and close it if all linked cursors are closed
    Cursor masterCursor = getMasterCursor();

    Set<FilteredCursor> linkedFilteredCursorSet = sMasterCursorMap.get(masterCursor);
    if (linkedFilteredCursorSet == null) {
      masterCursor.close(); // Shouldn't ever happen?
    } else {
      linkedFilteredCursorSet.remove(this);
      if (linkedFilteredCursorSet.isEmpty()) {
        masterCursor.close();
      }
    }

    if (masterCursor.isClosed()) {
      sMasterCursorMap.remove(masterCursor);
    }
  }

  @Override
  @Deprecated
  public boolean requery() {
    throw new UnsupportedOperationException();
  }

  private void attachToMasterCursor() {
    Cursor masterCursor = getMasterCursor();
    Set<FilteredCursor> filteredCursorSet = sMasterCursorMap.get(masterCursor);
    if (filteredCursorSet == null) {
      filteredCursorSet = Collections.synchronizedSet(new HashSet<FilteredCursor>());
      sMasterCursorMap.put(masterCursor, filteredCursorSet);
    }
    filteredCursorSet.add(this);
  }

  /** Returns the first non-CursorWrapper instance contained within this object. */
  public Cursor getMasterCursor() {
    Cursor cursor = mCursor;

    while (cursor instanceof CursorWrapper) {
      cursor = ((CursorWrapper) cursor).getWrappedCursor();
    }

    return cursor;
  }

  /** Returns the first FilteredCursor wrapped by the provided cursor or null if no FilteredCursor is found. */
  public static FilteredCursor unwrapFilteredCursor(Cursor cursor) {
    while (cursor instanceof CursorWrapper) {
      if (cursor instanceof FilteredCursor) {
        return (FilteredCursor)cursor;
      } else {
        cursor = ((CursorWrapper) cursor).getWrappedCursor();
      }
    }

    return null;
  }

}
