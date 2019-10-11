import com.google.gson.Gson;
import com.vmware.crs.crsmeutils.Models.MachineResource;
import com.vmware.crs.crsmeutils.SharedHardwareClient;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import java.util.List;
import java.util.Map;

public class NewConsumer extends Thread {
    public static AtomicInteger indexCallCount = new AtomicInteger(0);
    public static AtomicInteger successIndexCallCount = new AtomicInteger(0);
    public static AtomicInteger finishedIndexCount = new AtomicInteger(0);
    public static ConcurrentMap<Integer, Long> indexCallIntervals = new ConcurrentHashMap();

    public static AtomicInteger leaseCallCount = new AtomicInteger(0);
    public static AtomicInteger successLeaseCallCount = new AtomicInteger(0);
    public static AtomicInteger finishedLeaseCount = new AtomicInteger(0);
    public static ConcurrentMap<Integer, Long> leaseCallIntervals = new ConcurrentHashMap();

    public static AtomicInteger allocateCallCount = new AtomicInteger(0);
    public static AtomicInteger successAllocateCallCount = new AtomicInteger(0);
    public static AtomicInteger finishedAllocateCount = new AtomicInteger(0);
    public static ConcurrentMap<Integer, Long> allocateCallIntervals = new ConcurrentHashMap();

    public enum Action { INDEX, LEASE, ALLOCATE }

    final static String  HOST = "10.153.218.121";
    final static int PORT = 2343;

    public static List<MachineResource> getMachines() throws IOException, URISyntaxException {
        return new SharedHardwareClient(HOST, PORT).getAllMachines();
    }

    private Action action;
    private String catId;
    private SharedHardwareClient client;

    public NewConsumer(Action action) {
        this.action = action;
        this.client = new SharedHardwareClient(HOST, PORT);
    }

    public NewConsumer(Action action, String catId) {
        this.action = action;
        this.catId = catId;
        this.client = new SharedHardwareClient(HOST, PORT);
    }

    public void run() {
        switch (action) {
            case INDEX:
                callGetMachines();
                break;
            case LEASE:
                doAllLeaseActions(catId);
                break;
            case ALLOCATE:
                doAllAllocatinRelatedActions();
                break;
        }
    }

    private void callGetMachines() {
        indexCallCount.incrementAndGet();
        List<MachineResource> machines = null;
        try {
            long start = System.currentTimeMillis();
            System.out.println(Thread.currentThread().getName() + " begin to get machines");
            machines = client.getAllMachines();
            long offset = System.currentTimeMillis() - start;
            if(machines != null && machines.size() > 0) {
                System.out.println(Thread.currentThread().getName() + " get " + machines.size() + " machines");
                successIndexCallCount.incrementAndGet();
                indexCallIntervals.put(successIndexCallCount.get(), offset);
            } else {
                System.out.println(Thread.currentThread().getName() + " get machines failed.");
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        } finally {
            finishedIndexCount.incrementAndGet();
        }
    }

    private void doAllLeaseActions(String catId) {
        leaseCallCount.incrementAndGet();
        try {
            long start = System.currentTimeMillis();
            HashMap<String, String> params = new HashMap<String, String>();
            params.put("lease_owner", "yongwenz");
            System.out.println(Thread.currentThread().getName() + " begin to lease machine " + catId);
            CloseableHttpResponse leaseResponse = client.allocateMachinesForLease(catId, params);
            if (leaseResponse.getStatusLine().getStatusCode() == HttpStatus.SC_CREATED) {
                Map<String, String> responseJson = new Gson().fromJson(
                        new InputStreamReader(leaseResponse.getEntity().getContent()), Map.class);
                params.put("token", responseJson.get("leaseStartTime"));
                System.out.println(Thread.currentThread().getName() + " begin to extend lease for machine " + catId);
                CloseableHttpResponse extendLease = client.extendLeasePeriodOfMachines(catId, params);
                if (extendLease.getStatusLine().getStatusCode() == HttpStatus.SC_ACCEPTED) {
                    responseJson = new Gson().fromJson(
                            new InputStreamReader(extendLease.getEntity().getContent()), Map.class);
                    params.put("token", responseJson.get("leaseStartTime"));
                    System.out.println(Thread.currentThread().getName() + " begin to cancel lease for machine " + catId);
                    CloseableHttpResponse cancelResponse = client.cancelLeasePeriodOfMachines(catId, params);
                    if(cancelResponse.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                        long offset = System.currentTimeMillis() - start;
                        successLeaseCallCount.incrementAndGet();
                        leaseCallIntervals.put(successLeaseCallCount.get(), offset);
                    } else {
                        System.out.println(Thread.currentThread().getName() + " cancel lease for machine " + catId +
                                " failed with response " + EntityUtils.toString(cancelResponse.getEntity()));
                    }
                } else {
                    System.out.println(Thread.currentThread().getName() + " extend lease for machine " + catId +
                            " failed with response " + EntityUtils.toString(extendLease.getEntity()));
                }
            } else {
                System.out.println(Thread.currentThread().getName() + " lease machine " + catId +
                        " failed with response " + EntityUtils.toString(leaseResponse.getEntity()));
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        } finally {
            finishedLeaseCount.incrementAndGet();
        }
    }

    private void doAllAllocatinRelatedActions() {
        allocateCallCount.incrementAndGet();
        try {
            long start = System.currentTimeMillis();
            Map<String, String> params = new HashMap<String, String>();
            params.put("HOST_01_CATID", "");
            params.put("jobUrl", "http://crs.eng.vmware.com/job/abc");
            params.put("HOST_01_DATASTORENAME", "");
            System.out.println(Thread.currentThread().getName() + " begin to allocate machine");
            CloseableHttpResponse allocateResponse = client.allocateMachines(params);
            if (allocateResponse.getStatusLine().getStatusCode() == HttpStatus.SC_CREATED) {
                Map<String, Object> responseJson = new Gson().fromJson(
                        new InputStreamReader(allocateResponse.getEntity().getContent()), Map.class);
                List<String> catIds = new ArrayList<String>();
                for(String key : responseJson.keySet()) {
                    if(key.endsWith("CATID")) {
                        catIds.add((String)responseJson.get(key));
                    }
                }
                System.out.println(Thread.currentThread().getName() + " begin to deallocate machine " + catIds.get(0));
                CloseableHttpResponse deallocateResponse = client.deAllocateMachines(catIds);
                if (deallocateResponse.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                    long offset = System.currentTimeMillis() - start;
                    successAllocateCallCount.incrementAndGet();
                    allocateCallIntervals.put(successAllocateCallCount.get(), offset);
                } else {
                    System.out.println(Thread.currentThread().getName() + " deallocate machine " +
                            catIds.get(0) + " failed with response "
                            + EntityUtils.toString(allocateResponse.getEntity()));
                }
            } else {
                System.out.println(Thread.currentThread().getName() + " allocate machine failed with response "
                        + EntityUtils.toString(allocateResponse.getEntity()));
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        } finally {
            finishedAllocateCount.incrementAndGet();
        }

    }

}
