import ch.qos.logback.classic.spi.Configurator;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import org.slf4j.LoggerFactory;

import com.vmware.crs.crsmeutils.Models.MachineResource;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Set;
import java.util.Arrays;
import java.util.HashSet;

public class NewStatistics {
    public static void main(String[] strs) {

        Set<String> loggers = new HashSet<String>(Arrays.asList("org.apache.http", "groovyx.net.http"));
        for(String log:loggers) {
            Logger logger = (Logger) LoggerFactory.getLogger(log);
            logger.setLevel(Level.INFO);
            logger.setAdditive(false);
        }

        NewStatistics s = new NewStatistics();
        try {
//            s.callIndex(100);
//            s.callLease(200);
            s.callAllocate(200);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void callIndex(int count) throws InterruptedException {
        for (int i = 0; i < count; i++) {
            NewConsumer c = new NewConsumer(NewConsumer.Action.INDEX);
            c.start();
        }
        while (true) {
            Thread.sleep(2000);
            if (NewConsumer.finishedIndexCount.get() == NewConsumer.indexCallCount.get()) {
                long totalTime = 0;
                for (long l : NewConsumer.indexCallIntervals.values()) {
                    totalTime += l;
                }
                double avgTime = totalTime / NewConsumer.successIndexCallCount.get();
                System.out.println("New sharedpool get all machines " + NewConsumer.indexCallCount + " times "
                        + NewConsumer.successIndexCallCount + " success calls, used "
                        + totalTime + "ms, avg time is " + avgTime + "ms");
                break;
            }
        }
    }

    private void callLease(int count) throws InterruptedException, IOException, URISyntaxException {
        List<MachineResource> resourceList = NewConsumer.getMachines();
        for (int i = 0; i < count; i++) {
            int index = i % resourceList.size();
            NewConsumer c = new NewConsumer(NewConsumer.Action.LEASE, resourceList.get(index).getCatId());
            c.start();
        }

        while (true) {
            Thread.sleep(2000);
            if (NewConsumer.finishedLeaseCount.get() == NewConsumer.leaseCallCount.get()) {
                long totalTime = 0;
                for (long l : NewConsumer.leaseCallIntervals.values()) {
                    totalTime += l;
                }
                double avgTime = totalTime / NewConsumer.successLeaseCallCount.get();
                System.out.println("New sharedpool lease/extend_lease/cancel_lease machine "
                        + NewConsumer.leaseCallCount + " times "
                        + NewConsumer.successLeaseCallCount + " success calls, used "
                        + totalTime + "ms, avg time is " + avgTime + "ms");
                break;
            }
        }
    }

    private void callAllocate(int count) throws InterruptedException {
        for (int i = 0; i < count; i++) {
            NewConsumer c = new NewConsumer(NewConsumer.Action.ALLOCATE);
            c.start();
        }

        while (true) {
            Thread.sleep(2000);
            if (NewConsumer.finishedAllocateCount.get() == NewConsumer.allocateCallCount.get()) {
                long totalTime = 0;
                for (long l : NewConsumer.allocateCallIntervals.values()) {
                    totalTime += l;
                }
                double avgTime = totalTime / NewConsumer.successAllocateCallCount.get();
                System.out.println("New sharedpool allocate/deallocate machine "
                        + NewConsumer.allocateCallCount + " times "
                        + NewConsumer.successAllocateCallCount + " success calls, used "
                        + totalTime + "ms, avg time is " + avgTime + "ms");
                break;
            }
        }
    }
}
