import com.vmware.crs.crsmeutils.MachineResource;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;

public class OldStatistics {
    public static void main(String[] strs) {
        OldStatistics s = new OldStatistics();
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
            OldConsumer c = new OldConsumer(OldConsumer.Action.INDEX);
            c.start();
        }
        while (true) {
            Thread.sleep(2000);
            if (OldConsumer.finishedIndexCount.get() == OldConsumer.indexCallCount.get()) {
                long totalTime = 0;
                for (long l : OldConsumer.indexCallIntervals.values()) {
                    totalTime += l;
                }
                double avgTime = totalTime / OldConsumer.successIndexCallCount.get();
                System.out.println("Old sharedpool get all machines " + OldConsumer.indexCallCount + " times "
                        + OldConsumer.successIndexCallCount + " success calls, used "
                        + totalTime + "ms, avg time is " + avgTime + "ms");
                break;
            }
        }
    }

    private void callLease(int count) throws InterruptedException {
        List<MachineResource> resourceList = OldConsumer.getMachines();
        for (int i = 0; i < count; i++) {
            int index = i % resourceList.size();
            OldConsumer c = new OldConsumer(OldConsumer.Action.LEASE, resourceList.get(index));
            c.start();
        }

        while (true) {
            Thread.sleep(2000);
            if (OldConsumer.finishedLeaseCount.get() == OldConsumer.leaseCallCount.get()) {
                long totalTime = 0;
                for (long l : OldConsumer.leaseCallIntervals.values()) {
                    totalTime += l;
                }
                double avgTime = totalTime / OldConsumer.successLeaseCallCount.get();
                System.out.println("Old sharedpool lease/extend_lease/cancel_lease machine "
                        + OldConsumer.leaseCallCount + " times "
                        + OldConsumer.successLeaseCallCount + " success calls, used "
                        + totalTime + "ms, avg time is " + avgTime + "ms");
                break;
            }
        }
    }

    private void callAllocate(int count) throws InterruptedException, IOException, URISyntaxException {
        for (int i = 0; i < count; i++) {
            OldConsumer c = new OldConsumer(OldConsumer.Action.ALLOCATE);
            c.start();
        }

        while (true) {
            Thread.sleep(2000);
            if (OldConsumer.finishedAllocateCount.get() == OldConsumer.allocateCallCount.get()) {
                long totalTime = 0;
                for (long l : OldConsumer.allocateCallIntervals.values()) {
                    totalTime += l;
                }
                double avgTime = totalTime / OldConsumer.successAllocateCallCount.get();
                System.out.println("Old sharedpool allocate/deallocate machine "
                        + OldConsumer.allocateCallCount + " times "
                        + OldConsumer.successAllocateCallCount + " success calls, used "
                        + totalTime + "ms, avg time is " + avgTime + "ms");
                break;
            }
        }
    }
}
