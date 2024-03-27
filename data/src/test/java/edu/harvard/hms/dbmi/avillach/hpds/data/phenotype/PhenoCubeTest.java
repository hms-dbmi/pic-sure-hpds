package edu.harvard.hms.dbmi.avillach.hpds.data.phenotype;

import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class PhenoCubeTest {

    @Test
    public void shouldGetValueWhenLastKeyMatches() {
        KeyAndValue[] sortedKeyValuePairs = {
            new KeyAndValue<>(1, "A01.00 Typhoid fever, unspecified")
        };

        PhenoCube<?> subject = new PhenoCube<>("bob", KeyAndValue.class);
        subject.setSortedByKey(sortedKeyValuePairs);

        List<? extends KeyAndValue<?>> actual = subject.getValuesForKeys(List.of(1));
        List<KeyAndValue<?>> expected = List.of(sortedKeyValuePairs[0]);

        Assert.assertEquals(expected, actual);
    }

    @Test
    public void shouldWorkForEmptyList() {
        KeyAndValue[] sortedKeyValuePairs = {};

        PhenoCube<?> subject = new PhenoCube<>("bob", KeyAndValue.class);
        subject.setSortedByKey(sortedKeyValuePairs);

        List<? extends KeyAndValue<?>> actual = subject.getValuesForKeys(List.of(1));
        List<KeyAndValue<?>> expected = List.of();

        Assert.assertEquals(expected, actual);
    }

    @Test
    public void shouldGetValuesWhenPatientMatchesSeveralKeys() {
        KeyAndValue[] pairs = {
            new KeyAndValue<>(0, ":)"),
            new KeyAndValue<>(1, "A99.9 Not actually hungry, just bored"),
            new KeyAndValue<>(1, "A99.99 Feeling snackish"),
            new KeyAndValue<>(1, "A99.999 Legit hungry"),
            new KeyAndValue<>(1, "A99.9999 FOOD FOOD FOOD FOOD FOOD FOOD"),
            new KeyAndValue<>(2, ">:|"),
        };

        PhenoCube<?> subject = new PhenoCube<>("bob", KeyAndValue.class);
        subject.setSortedByKey(pairs);

        List<? extends KeyAndValue<?>> actual = subject.getValuesForKeys(List.of(1));
        List<KeyAndValue<?>> expected = List.of(pairs[1], pairs[2], pairs[3], pairs[4]);

        Assert.assertEquals(expected, actual);
    }

    @Test
    public void shouldGetValuesWhenNoneMatch() {
        KeyAndValue[] pairs = {
            new KeyAndValue<>(0, ":)"),
            new KeyAndValue<>(2, ">:|"),
        };

        PhenoCube<?> subject = new PhenoCube<>("bob", KeyAndValue.class);
        subject.setSortedByKey(pairs);

        List<? extends KeyAndValue<?>> actual = subject.getValuesForKeys(List.of(1));
        List<KeyAndValue<?>> expected = List.of();

        Assert.assertEquals(expected, actual);
    }

    @Test
    public void shouldGetValuesWithSparseMatches() {
        KeyAndValue[] pairs = {
            new KeyAndValue<>(0, ":)"),
            new KeyAndValue<>(1, ":o"),
            new KeyAndValue<>(2, ">:|"),
            new KeyAndValue<>(2, ":|"),
            new KeyAndValue<>(3, ":]"),
            new KeyAndValue<>(3, ":["),
        };

        PhenoCube<?> subject = new PhenoCube<>("bob", KeyAndValue.class);
        subject.setSortedByKey(pairs);

        List<? extends KeyAndValue<?>> actual = subject.getValuesForKeys(List.of(1, 3));
        List<KeyAndValue<?>> expected = List.of(pairs[1], pairs[4], pairs[5]);

        Assert.assertEquals(expected, actual);
    }
}