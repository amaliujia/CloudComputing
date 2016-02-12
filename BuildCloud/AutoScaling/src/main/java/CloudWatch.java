import org.openstack4j.api.OSClient;
import org.openstack4j.model.compute.Server;
import org.openstack4j.model.telemetry.Statistics;
import org.openstack4j.openstack.OSFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;

/**
 * Created by amaliujia on 16-2-8.
 */
public class CloudWatch {

    public final Map<String, String> params;
    public final List<DataCenterInstance> instances;
    public final FlagWrapper wrapper;

    public static OSClient os;

    public CloudWatch(Map<String, String> params,  List<DataCenterInstance> instances, FlagWrapper wrapper) {
        this.params = params;
        // this.os = os;
        this.instances = instances;
        this.wrapper = wrapper;
        this.verify();
    }

    private void verify() {
        String username = this.params.get("USER_NAME");
        String password = this.params.get("PASSWORD");
        String localAddr = null;
        try {
            localAddr = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

        os = OSFactory.builder()
                .endpoint("http://" + localAddr +":5000/v2.0")
                .credentials(username, password)
                .tenantName(AutoScalingService.params.get("PROJECT_NAME"))
                .authenticate();
    }

    public void start() {
        int cooldown = Integer.parseInt(params.get("COOLDOWN"));
        int period = Integer.parseInt(params.get("EVAL_PERIOD"));
        int count = Integer.parseInt(params.get("EVAL_COUNT"));
        double scale_down = Double.parseDouble(params.get("CPU_LOWER_TRES"));
        double scale_up = Double.parseDouble(params.get("CPU_UPPER_TRES"));
        double delta = Integer.parseInt(params.get("DELTA"));

        String LB_IP = params.get("LB_IPADDR");

        String min_instance_str = params.get("MIN_INSTANCE");
        int min_instance = Integer.parseInt(min_instance_str);
        String max_instace_str = params.get("MAX_INSTANCE");
        int max_instace = Integer.parseInt(max_instace_str);

        int scale_up_count = 0;
        int scale_down_count = 0;

        try{
            //Thread.sleep(1000 * cooldown);
            while (!wrapper.isShutdown) {
                List<? extends Statistics> stat = os.telemetry().meters().statistics("cpu_util", period);
                double avg = 0.0;
                System.out.println("\n\n");
                double j = 0;
                for(int i = stat.size() - 1; i >= 0 && j < 3.0; i--) {
                    System.out.println("    " + stat.get(i).getDuration() + "  " + stat.get(i).getAvg());
                    avg += stat.get(i).getAvg();
                    j += 1.0;
                }

                avg /= j;
                System.out.println("CPU: " + avg);

                System.out.println("\n\n");

                if (avg >= scale_up) {
                    // update
                    scale_up_count++;
                } else if(avg <= scale_down) {
                    // update
                    scale_down_count++;
                } else {
                    scale_down_count = 0;
                    scale_up_count = 0;
                }

                if (scale_up_count >= count || scale_down_count >= count) {
                    int z = 0;
                    if (scale_down_count >= count && instances.size() > min_instance && z < delta) {
                        String server_id = instances.get(0).getServerID();
                        System.out.println("\nStop server " + server_id + "\n");
                        AutoScalingService.killServer(server_id);
                        HttpConnections.removeInstanceFromLoadBalancer(LB_IP, instances.get(0));
                        instances.remove(0);
                        z++;
                    } else if (scale_up_count >= count && instances.size() < max_instace && z < delta){
                        Server server = AutoScalingService.launchServer();
                        System.out.println("\nStart server " + server.getId() + "\n");
                        DataCenterInstance instance = new DataCenterInstance(server);
                        HttpConnections.addInstanceToLoadBalancer(LB_IP, instance);
                        instances.add(instance);
                        z++;
                    }

                    if (z == delta && scale_down_count >= count) {
                        Thread.sleep(1000 * cooldown);
                    }
                }

                Thread.sleep(1000 * period);
            }
        } catch (InterruptedException e) {
            System.exit(1);
        }
    }
}
