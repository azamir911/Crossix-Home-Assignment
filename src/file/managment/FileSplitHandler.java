package file.managment;

import comparator.StringToLongComparator;
import sorting.ArraySorter;
import sorting.StringArraySorter;

import java.io.*;
import java.util.Queue;
import java.util.UUID;

/**
 * Splitting the given file for a small files based on the max memory.
 * Put the new files in the queue.
 * Sorting the file based on the key index field
 */
public class FileSplitHandler {

    private final File file;
    private final int maxMemory;
    private final int keyIndex;

    public FileSplitHandler(File file, int maxMemory, int keyIndex) {
        this.file = file;
        this.maxMemory = maxMemory;
        this.keyIndex = keyIndex;
    }

    public void split(Queue<File> queue) throws IOException {
        // Go over the base file and split it to small files based in the memory size
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String line = reader.readLine();
            int counter = 0;
            String[] array = new String[maxMemory];
            array[counter] = line;
            counter++;
            while(line != null) {
                if (counter == maxMemory) {
                    addFile(queue, array);

                    array = new String[maxMemory];
                    counter = 0;
                }
                line = reader.readLine();
                array[counter] = line;
                counter++;
            }

            if (array[0] != null) {
                addFile(queue, array);
            }
        }
    }

    private void addFile(Queue<File> queue, String[] array) throws IOException {
        ArraySorter arraySorter = new StringArraySorter(array, new StringToLongComparator(this.keyIndex));
        arraySorter.sort();
        File file = new File(UUID.randomUUID()+".csv");
        try (BufferedWriter writer = new BufferedWriter((new FileWriter(file)))) {
            for (int i =0; i < array.length && array[i] != null; i++) {
                writer.write(array[i] + System.lineSeparator());
            }
        }
        queue.add(file);
    }

}
