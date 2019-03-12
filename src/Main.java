import service.FileSortingService;

import java.io.*;
import java.nio.charset.Charset;
import java.util.Random;
import java.util.UUID;

public class Main {

    public static void main(String[] args) {
        File file = new File("C:\\Dev\\FileSorting\\src\\file2.csv");

        FileSortingService fileSortingService = new FileSortingService(file, 10, 1000, 0);

        final long start = System.currentTimeMillis();
        try {
            fileSortingService.execute();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        final long end = System.currentTimeMillis();

        System.out.println("Duration: " + (end - start));

//        Random r = new Random();
//        File file = new File("C:\\Dev\\FileSorting\\src\\file1.csv");
//
//        try (BufferedWriter writer = new BufferedWriter((new FileWriter(file)))) {
//            for (int i = 0; i < 100000; i++) {
//                final int num = r.nextInt((1_000_000_000 - 1_000_000) + 1) + 1_000_000;
//                String uuid = UUID.randomUUID().toString();
//                writer.write(num + "," + uuid + "" + System.lineSeparator());
//            }
//        }
//        catch (IOException e) {
//            e.printStackTrace();
//        }


    }
}
