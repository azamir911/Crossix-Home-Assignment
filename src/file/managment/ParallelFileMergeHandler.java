package file.managment;

import comparator.StringToLongComparator;

import java.io.File;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ParallelFileMergeHandler {

    private final int keyIndex;
    private final int threads;
    private final Collection<CompletableFuture<File>> futures = new ConcurrentLinkedQueue<>();

    public ParallelFileMergeHandler(int keyIndex, int threads) {
        this.keyIndex = keyIndex;
        this.threads = threads;
    }

    public void merge(Queue<File> queue) {
        final ExecutorService executorService = Executors.newFixedThreadPool(threads);

        // Start merging the files in the queue
        // We can use the same queue but should know when to stop, or create a new queue for each iterator and the
        // last queue is with one file so this is the sorted file
        while (true) {
            File first = getFile(queue);
            File second = getFile(queue);

            // This is the condition to stop the loop
            if (second == null) {
                System.out.println("Done merging. Adding the file " + first.getName() + " to the queue");
                queue.add(first);
                executorService.shutdownNow();
                return;
            }

            Comparator comparator = new StringToLongComparator(this.keyIndex);
            System.out.println("Merging " + first.getName() + " with " + second.getName());
            FileMerger fileMerger = new FileMerger(first, second, comparator);

            // Run the task async
            CompletableFuture<File> completableFuture = CompletableFuture
                    .supplyAsync(() -> fileMerger.call(), executorService)
                    .whenComplete((file, ex) -> doComplete(file, fileMerger, queue, ex));

            System.out.println("Adding future to list");
            futures.add(completableFuture);
        }

    }

    // Getting the next file in the queue.
    // If the queue is empty, checking if any thread is working and waiting for the first time that will finish
    // and store the merge file in the queue.
    private File getFile(Queue<File> queue) {
        File file = null;
        if (!queue.isEmpty()) {
            file = queue.poll();
            System.out.println("Get file: " + file.getName());
        }
        else {
            if (!clearDone().isEmpty()) {
                while (queue.isEmpty()) {
                    // Waiting till a future will be finished and the queue will contains a file
                }
                file = queue.poll();
                System.out.println("Get file after waiting: " + file.getName());
            }
        }
        return file;
    }

    private void doComplete(File file, FileMerger fileMerger, Queue<File> queue, Throwable ex) {
        System.out.println("Deleting " + fileMerger.getFirst().getName());
        fileMerger.getFirst().delete();
        System.out.println("Deleting " + fileMerger.getSecond().getName());
        fileMerger.getSecond().delete();
        System.out.println("Adding " + file.getName() + " to the queue");
        queue.add(file);
    }

    private Collection<CompletableFuture<File>> clearDone() {
        final Iterator<CompletableFuture<File>> iterator = futures.iterator();
        while (iterator.hasNext()) {
            final CompletableFuture<File> next = iterator.next();
            if (next.isDone()) {
                iterator.remove();
            }
        }

        return futures;
    }


}
