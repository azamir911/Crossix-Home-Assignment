package file.managment;

import comparator.StringToLongComparator;

import java.io.*;
import java.util.Comparator;
import java.util.UUID;
import java.util.concurrent.Callable;

/**
 * Merge 2 files into one file based on the
 */
public class FileMerger implements Callable<File> {

    private final File first;
    private final File second;
    private final Comparator comparator;

    public FileMerger(File first, File second, Comparator comparator) {
        this.first = first;
        this.second = second;
        this.comparator = comparator;
    }

    @Override
    public File call()  {
        System.out.println("Start merge with " + Thread.currentThread().getName() +  " " + first.getName() + " - " + second.getName());

        File file = new File(UUID.randomUUID()+".csv");
        try (BufferedReader reader1 = new BufferedReader(new FileReader(first));
             BufferedReader reader2 = new BufferedReader(new FileReader(second));
             BufferedWriter writer = new BufferedWriter((new FileWriter(file)))) {

            String line1 = reader1.readLine();
            String line2 = reader2.readLine();

            while (line1 != null & line2 != null) {
                final int compare = comparator.compare(line1, line2);
                if (compare == 0 || compare == -1) {
                    writer.write(line1 + System.lineSeparator());
                    line1 = reader1.readLine();
                }
                else if (compare == 1) {
                    writer.write(line2 + System.lineSeparator());
                    line2 = reader2.readLine();
                }
            }

            while (line1 != null) {
                writer.write(line1 + System.lineSeparator());
                line1 = reader1.readLine();
            }

            while (line2 != null) {
                writer.write(line2 + System.lineSeparator());
                line2 = reader2.readLine();
            }
        }
        catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("End merge with " + Thread.currentThread().getName() +  " " + first.getName() + " - " + second.getName());
        return file;
    }

    public File getFirst() {
        return this.first;
    }

    public File getSecond() {
        return this.second;
    }
}
