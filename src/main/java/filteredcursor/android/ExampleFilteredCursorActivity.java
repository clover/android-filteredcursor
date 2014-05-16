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

import android.app.Activity;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.os.Bundle;
import android.widget.ListView;
import android.widget.SimpleCursorAdapter;

public class ExampleFilteredCursorActivity extends Activity {

  @Override
  public void onCreate(Bundle savedInstanceState) {
    super.onCreate(savedInstanceState);
    setContentView(R.layout.main);

    MatrixCursor source = new MatrixCursor(new String[] {"_id", "name"});
    source.addRow(new Object[] { 0l, "Alpha" });
    source.addRow(new Object[] { 1l, "Beta" });
    source.addRow(new Object[] { 2l, "Omega" });
    source.addRow(new Object[] { 3l, "Beta" });

    // Filter out "Beta"
    FilteredCursor filtered = FilteredCursor.createUsingSelector(source, new FilteredCursor.Selector() {
      int nameIndex = -1;

      @Override
      public boolean select(Cursor cursor) {
        if (nameIndex == -1) {
          nameIndex = cursor.getColumnIndex("name");
        }

        if ("Beta".equals(cursor.getString(nameIndex))) {
          return false;
        } else {
          return true;
        }
      }
    });

    // Swap the first two rows
    filtered.swapItems(0, 1);

    // Filter the FilteredCursor
    filtered = FilteredCursor.createUsingFilter(filtered, new int[] { 0, 1, 0, 1, 0, 1 });

    SimpleCursorAdapter adapter = new SimpleCursorAdapter(
        this,
        android.R.layout.simple_list_item_1,
        filtered,
        new String[] { "name" },
        new int[] { android.R.id.text1 },
        0
    );

    ListView listView = (ListView)findViewById(R.id.list);
    listView.setAdapter(adapter);
  }

}
