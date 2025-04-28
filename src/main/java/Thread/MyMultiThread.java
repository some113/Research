package Thread;

import com.squareup.okhttp.Call;
import com.sun.research.ws.wadl.Link;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.*;

public class MyMultiThread {
    //Task
    static LinkedList<Callable<?>> tasks = new LinkedList<>();

    static long taskTimeout;
//    static TimeUnit timeUnit;
    static int maxThreadCount = 5;
    static int activeThreadCount = 0;
    static ArrayList<MyThreadController> currentThreadControllers = new ArrayList<>(); // currentThreads
    static ExecutorService executor = null;

    public MyMultiThread(int taskTimeout, int maxThreadCount) {
        this.taskTimeout = taskTimeout;
        this.maxThreadCount = maxThreadCount;
        executor = Executors.newFixedThreadPool(maxThreadCount);
    }

    public void addTask(Callable<?> task) {
        tasks.add(task);
    }

    public static void run() {
        while (!tasks.isEmpty()) {
            for (int i = 0; i < currentThreadControllers.size(); i++) {
                MyThreadController controller = currentThreadControllers.get(i);
//                if (System.currentTimeMillis() - controller.startTime > taskTimeout) {
//                    controller.thread.interrupt();
//                    currentThreadControllers.remove(i);
//                    System.out.println("Task timeout");
//                } else
                    if (!controller.thread.isAlive()) {
                    controller.thread.interrupt();
                    currentThreadControllers.remove(i);
                }
            }
            if (currentThreadControllers.size() < maxThreadCount && !tasks.isEmpty()) {
                Callable<?> task = tasks.poll();
                Future<?> future = executor.submit(task);
//                activeThreadCount++;
                MyThreadController controller = null;
                Thread thread = new Thread(() -> {
                    try {
                        future.get(taskTimeout, TimeUnit.MILLISECONDS);
                    } catch (Exception e) {
                        e.printStackTrace();
                        System.out.println("Unable to execute task: " + e.getMessage());
                        Thread.currentThread().interrupt();
                    } finally {
//                        activeThreadCount--;
                    }
                });
                controller = new MyThreadController(thread);
                controller.run();
                currentThreadControllers.add(controller);
            }
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        while (!currentThreadControllers.isEmpty()) {
            MyThreadController controller = currentThreadControllers.get(0);
            if (!controller.thread.isAlive()) {
                controller.thread.interrupt();
                currentThreadControllers.remove(0);
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
class MyThreadController {
    Thread thread;
    long startTime;

    MyThreadController(Thread thread) {
        this.thread = thread;
    }

    public void run() {
        startTime = System.currentTimeMillis();
        thread.start();
    }
}
