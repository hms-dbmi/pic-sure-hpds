package edu.harvard.hms.dbmi.avillach.hpds.processing;


import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class ColumnSorterTest {

    @Test
    public void shouldSortColumns() {
        ColumnSorter subject = new ColumnSorter(List.of("a", "b", "c"));
        List<String> actual = subject.sortInfoColumns(new ArrayList<>(List.of("b", "c", "a")));
        List<String> expected = List.of("a", "b", "c");

        Assert.assertEquals(expected, actual);
    }

    @Test
    public void shouldExcludeMissingColumns() {
        ColumnSorter subject = new ColumnSorter(List.of("a", "b", "c"));
        List<String> actual = subject.sortInfoColumns(new ArrayList<>(List.of("d", "b", "c", "a")));
        List<String> expected = List.of("a", "b", "c");

        Assert.assertEquals(expected, actual);
    }

    @Test
    public void shouldNotBreakForMissingColumns() {
        ColumnSorter subject = new ColumnSorter(List.of("a", "b", "c", "d"));
        List<String> actual = subject.sortInfoColumns(new ArrayList<>(List.of("d", "a")));
        List<String> expected = List.of("a", "d");

        Assert.assertEquals(expected, actual);
    }

    @Test
    public void shouldNoOpWithoutConfig() {
        ColumnSorter subject = new ColumnSorter(List.of());
        List<String> actual = subject.sortInfoColumns(new ArrayList<>(List.of("b", "c", "a")));
        List<String> expected = List.of("b", "c", "a");

        Assert.assertEquals(expected, actual);
    }
}