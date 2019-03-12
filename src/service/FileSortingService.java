package service;

import file.managment.FileSplitHandler;
import file.managment.ParallelFileMergeHandler;

import java.io.File;
import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

public class FileSortingService {

    private final int threads;
    private final int maxMemory;
    private final int keyIndex;
    private File file;

    public FileSortingService(File file, int threads, int maxMemory, int keyIndex) {
        this.file = file;
        this.threads = threads;
        this.maxMemory = maxMemory;
        this.keyIndex = keyIndex;
    }

    public File execute() throws Exception {
        Queue<File> queue = new LinkedBlockingQueue<>();
        splitFile(queue);
        mergeFiles(queue);
        final File poll = queue.poll();
        System.out.println(" Got the final file " + poll.getName());
        poll.renameTo(new File(this.file.getName() + ".sorted"));
        poll.delete();
        return poll;
    }

    private void mergeFiles(Queue<File> queue) {
//        FileMergeHandler fileMergeHandler = new FileMergeHandler(keyIndex);
        ParallelFileMergeHandler fileMergeHandler = new ParallelFileMergeHandler(keyIndex, threads);
        fileMergeHandler.merge(queue);
    }

    private void splitFile(Queue<File> queue) throws IOException {
        FileSplitHandler fileSplitHandler = new FileSplitHandler(file, maxMemory, keyIndex);
        fileSplitHandler.split(queue);
    }

}

