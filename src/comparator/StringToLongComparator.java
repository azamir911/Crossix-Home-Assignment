package comparator;

import java.util.Comparator;

/**
 * Comparator for string line with a long field index to be compared with
 */
public class StringToLongComparator implements Comparator<String> {

    private final int fieldIndex;

    public StringToLongComparator(int fieldIndex) {
        this.fieldIndex = fieldIndex;
    }

    @Override
    public int compare(String o1, String o2) {
        if (o1 == null && o2 == null) {
            return 0;
        }
        else if (o1 == null) {
            return 1;
        }
        else if (o2 == null) {
            return -1;
        }

        final Long value1 = getValue(o1);
        final Long value2 = getValue(o2);

        return value1.compareTo(value2);
    }

    private Long getValue(String str) {
        final String[] array = str.split(",");
        String strValue = array[this.fieldIndex];
        return Long.parseLong(strValue);
    }
}
