package sorting;

import java.util.Arrays;
import java.util.Comparator;

public abstract class ArraySorter<T> {

    private T[] array;
    private final Comparator<T> comparable;

    public ArraySorter(T[] array, Comparator<T> comparable) {
        this.array = array;
        this.comparable = comparable;
    }

    public void sort() {
        Arrays.sort(this.array, comparable);
    }
}
