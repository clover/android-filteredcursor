Android-FilteredCursor
======================

FilteredCursor is a CursorWrapper for Android that allows positions in a Cursor to be filtered out, repeated, or
reordered.

It requires Android API Level 11, but can easily be adapted to work on older versions as needed.

Usage
-----

The most elemental use of a FilteredCursor would be to provide a integer array specifying an arrangement of rows.
In this example the FilteredCursor swaps the first two rows from the source Cursor and filters out all other rows.

    Cursor filtered = FilteredCursor.createUsingFilter(Cursor, new int[] { 1, 0 });

License
=======

    Copyright (C) 2014 Clover Network, Inc.
 
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at
 
        http://www.apache.org/licenses/LICENSE-2.0
 
    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
