package file.managment;

import comparator.StringToLongComparator;

import java.io.File;
import java.util.Comparator;
import java.util.Queue;

/*
* Merging all the files that in the queue to one file that will be stored in the given queue.
* Comparing the string lines based on the key index field
*/
public class FileMergeHandler {

    private final int keyIndex;

    public FileMergeHandler(int keyIndex) {
        this.keyIndex = keyIndex;
    }

    public void merge(Queue<File> queue) throws Exception {
        // Start merging the files in the queue
        // We can use the same queue but should know when to stop, or create a new queue for each iterator and the
        // last queue is with one file so this is the sorted file
        while (!queue.isEmpty()) {
            File first = queue.poll();
            if (queue.isEmpty()) {
                queue.add(first);
                return;
            }
            File second = queue.poll();

            Comparator comparator = new StringToLongComparator(this.keyIndex);

            FileMerger fileMerger = new FileMerger(first, second, comparator);

            File merge = fileMerger.call();
            first.delete();
            second.delete();
            queue.add(merge);
        }
    }

}
