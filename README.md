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