import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;


public class Main {
    public static AtomicInteger incc = new AtomicInteger();

    public static void main(String[] args)
    {
//        ExecutorService pool = new ThreadPoolExecutor(1, 3, 1000,
//                TimeUnit.MILLISECONDS,
//                new SynchronousQueue<Runnable>(),
//                Executors.defaultThreadFactory(),
//                new ThreadPoolExecutor.AbortPolicy());
        ExecutorService pool2 = Executors.newSingleThreadExecutor();
        ExecutorService pool = new ThreadPoolExecutor(5, 5, 1000,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<Runnable>(8),
                Executors.defaultThreadFactory(),
                new ThreadPoolExecutor.AbortPolicy());
        for (int i = 0;i < 14;i++)
        {
            pool.execute(new myRunnable(incc));
        }
    }
}

class myRunnable implements Runnable
{
    public AtomicInteger incc;
    public SimpleDateFormat formatter = new SimpleDateFormat ("yyyy-MM-dd HH:mm:ss");
    public myRunnable(AtomicInteger incc)
    {
        this.incc = incc;
    }
    @Override
    public void run() {
        incc.incrementAndGet();
        System.out.println(formatter.format(new Date())+">>"+Thread.currentThread().getName()+","+incc.get());
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
