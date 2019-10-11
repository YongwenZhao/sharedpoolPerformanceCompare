import com.google.gson.Gson;
import com.vmware.crs.crsmeutils.MachineResource;
import com.vmware.crs.crsmeutils.SharedHardwareClient;
import com.vmware.crs.crsmeutils.SharedHardwareConstants;
import com.vmware.crs.crsmeutils.SharedHardwareRequest;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import java.util.List;
import java.util.Map;

public class OldConsumer extends Thread {
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

    final static String  HOST = "10.153.218.120";
    final static int PORT = 2343;
    final static int REST_PORT = 8080;

    public static List<MachineResource> getMachines() {
        SharedHardwareRequest r = new SharedHardwareClient(HOST, PORT).getAllHardware();
        return r.getResMachineList();
    }

    private Action action;
    private MachineResource machine;
    private SharedHardwareClient client;
    private CloseableHttpClient httpClient = HttpClients.createDefault();

    public OldConsumer(Action action) {
        this.action = action;
        this.client = new SharedHardwareClient(HOST, PORT);
    }

    public OldConsumer(Action action, MachineResource machine) {
        this.action = action;
        this.machine = machine;
        this.client = new SharedHardwareClient(HOST, PORT);
    }

    public void run() {
        switch (action) {
            case INDEX:
                callGetMachines();
                break;
            case LEASE:
                doAllLeaseActions(machine);
                break;
            case ALLOCATE:
                doAllAllocatinRelatedActions();
                break;
        }
    }

    private void callGetMachines() {
        indexCallCount.incrementAndGet();
        try {
            long start = System.currentTimeMillis();
            System.out.println(Thread.currentThread().getName() + " begin to get machines");
            SharedHardwareRequest r = client.getAllHardware();
            List<MachineResource> machines = r.getResMachineList();
            long offset = System.currentTimeMillis() - start;
            if(machines != null && machines.size() > 0) {
                System.out.println(Thread.currentThread().getName() + " get " + machines.size() + " machines");
                successIndexCallCount.incrementAndGet();
                indexCallIntervals.put(successIndexCallCount.get(), offset);
            } else {
                System.out.println(Thread.currentThread().getName() + " get machines failed with response type " +
                    r.getNetworkResponse());
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        } finally {
            finishedIndexCount.incrementAndGet();
        }
    }

    private void doAllLeaseActions(MachineResource machine) {
        leaseCallCount.incrementAndGet();
        try {
            long start = System.currentTimeMillis();
            String user = "yongwenz";
            String catId = machine.getHwMachine().getCatId();
            System.out.println(Thread.currentThread().getName() + " begin to lease machine " + catId);
            long token = doSafeLease(machine, user);
            if (token != 0) {
                System.out.println(Thread.currentThread().getName() + " begin to extend lease for machine " + catId);
                token = doSafeExtendLease(machine);
                if (token != 0) {
                    System.out.println(Thread.currentThread().getName() + " begin to cancel lease for machine " + catId);
                    boolean r = doSafeCancelLease(machine);
                    if(r) {
                        long offset = System.currentTimeMillis() - start;
                        successLeaseCallCount.incrementAndGet();
                        leaseCallIntervals.put(successLeaseCallCount.get(), offset);
                    }
                }
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
            params.put("jobName", "http://crs.eng.vmware.com/job/abc");
            params.put("HOST_01_DATASTORENAME", "");
            System.out.println(Thread.currentThread().getName() + " begin to allocate machine");
            CloseableHttpResponse allocateResponse = allocateMachines(params);
            if (allocateResponse.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                Map<String, Object> responseJson = new Gson().fromJson(
                        new InputStreamReader(allocateResponse.getEntity().getContent()), Map.class);
                List<String> catIds = new ArrayList<String>();
                for(String key : responseJson.keySet()) {
                    if(key.endsWith("CATID")) {
                        catIds.add((String)responseJson.get(key));
                    }
                }

                String strCatIds = String.join(",", catIds);
                System.out.println(Thread.currentThread().getName() + " begin to deallocate machine " + strCatIds);
                CloseableHttpResponse deallocateResponse = deallocate(strCatIds);
                if (deallocateResponse.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                    long offset = System.currentTimeMillis() - start;
                    successAllocateCallCount.incrementAndGet();
                    allocateCallIntervals.put(successAllocateCallCount.get(), offset);
                } else {
                    System.out.println(Thread.currentThread().getName() + " deallocate machine " +
                            strCatIds + " failed with response " +
                            EntityUtils.toString(deallocateResponse.getEntity()));
                }
            } else {
                System.out.println(Thread.currentThread().getName() + " allocate machine failed with response " +
                        EntityUtils.toString(allocateResponse.getEntity()));
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        } finally {
            finishedAllocateCount.incrementAndGet();
        }

    }

    private long doSafeLease(MachineResource machine, String user) {

        List<MachineResource> leaseMachineList = new ArrayList<MachineResource>();
        leaseMachineList.add(machine);
        machine.getLeaseInfo().setLeasedOwner(user);
        SharedHardwareRequest leaseReq = client.leaseMachines(leaseMachineList);
        if (leaseReq.getNetworkResponse().equals(SharedHardwareConstants.ResponseType.SUCCESS_OK.toString())) {
            List<MachineResource> leased = leaseReq.getResMachineList();
            if (leased != null && leased.size() > 0) {
                return leased.get(0).getLeaseInfo().getLeaseStartTime();
            }
        }
        System.out.println(Thread.currentThread().getName() + " lease machine " + machine.getHwMachine().getCatId() +
                " failed with response type " + leaseReq.getNetworkResponse());
        return 0;
    }

    private boolean doSafeCancelLease(MachineResource machine) {
        List<MachineResource> leasedMachineList = new ArrayList<MachineResource>();
        leasedMachineList.add(machine);
        SharedHardwareRequest deallocateReq =
                client.cancelLeasePeriodOfMachines(leasedMachineList);
        if (deallocateReq.getNetworkResponse().equals(SharedHardwareConstants.ResponseType.SUCCESS_OK.toString())) {
            return true;
        }
        System.out.println(Thread.currentThread().getName() + " cancel lease for machine " + machine.getHwMachine().getCatId() +
                " failed with response type " + deallocateReq.getNetworkResponse());
        return false;
    }

    private long doSafeExtendLease(MachineResource machine) {
        List<MachineResource> leasedMachineList = new ArrayList<MachineResource>();
        leasedMachineList.add(machine);
        SharedHardwareRequest extendLeaseReq =
                client.extendLeasePeriodOfMachines(leasedMachineList);
        if (extendLeaseReq.getNetworkResponse().equals(SharedHardwareConstants.ResponseType.SUCCESS_OK.toString())) {
            List<MachineResource> leased = extendLeaseReq.getResMachineList();
            if (leased != null && leased.size() > 0) {
                return leased.get(0).getLeaseInfo().getLeaseStartTime();
            }
        }
        System.out.println(Thread.currentThread().getName() + " extend lease for machine " + machine.getHwMachine().getCatId() +
                " failed with response type " + extendLeaseReq.getNetworkResponse());
        return 0;
    }

    private CloseableHttpResponse allocateMachines(Map<String, String> params)
            throws URISyntaxException, IOException {
        URI uri = new URIBuilder()
                .setScheme("http")
                .setHost(HOST)
                .setPort(REST_PORT)
                .setPath("/allocate")
                .build();
        HttpPost httpPost = new HttpPost(uri);
        httpPost.setHeader("Accept", "application/json");
        httpPost.setHeader("Content-type", "application/json");
        HttpEntity entity = new StringEntity(new Gson().toJson(params));
        httpPost.setEntity(entity);
        CloseableHttpResponse httpResponse = httpClient.execute(httpPost);
        return httpResponse;
    }

    private CloseableHttpResponse deallocate(String catIds) throws URISyntaxException, IOException {
        URI uri = new URIBuilder()
                .setScheme("http")
                .setHost(HOST)
                .setPort(REST_PORT)
                .setPath("/deallocate")
                .addParameter("catIds", catIds)
                .build();

        HttpPost postRequest = new HttpPost(uri);
        CloseableHttpResponse httpResponse = httpClient.execute(postRequest);

        return httpResponse;
    }

}
