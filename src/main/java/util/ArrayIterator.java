package util;

import java.util.Iterator;

public class ArrayIterator<T> implements Iterator<T> {

    private T[] array;
    private int curPos = 0;

    public ArrayIterator(T[] array) {
        this.array = array;
    }

    @Override
    public boolean hasNext() {
        return curPos < array.length;
    }

    @Override
    public T next() {
        T ele = array[curPos];
        curPos++;
        return ele;
    }
}