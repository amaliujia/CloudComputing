import org.openstack4j.api.OSClient;
import org.openstack4j.model.telemetry.Statistics;

import java.util.List;
import java.util.Map;

/**
 * Created by amaliujia on 16-2-8.
 */
public class CloudWatch {

    public final Map<String, String> params;
    public final OSClient os;
    public final List<DataCenterInstance> instances;
    public final FlagWrapper wrapper;

    public CloudWatch(Map<String, String> params, OSClient os, List<DataCenterInstance> instances, FlagWrapper wrapper) {
        this.params = params;
        this.os = os;
        this.instances = instances;
        this.wrapper = wrapper;
    }


    public void start() {
        int cooldown = Integer.parseInt(params.get("COOLDOWN"));
        int period = Integer.parseInt(params.get("EVAL_PERIOD"));
        int count = Integer.parseInt(params.get("EVAL_COUNT"));
        int scale_up = Integer.parseInt(params.get("CPU_LOWER_TRES"));
        int scale_down = Integer.parseInt(params.get("CPU_UPPER_TRES"));
        try{
            Thread.sleep(1000 * cooldown);
            while (!wrapper.isShutdown) {
                List<? extends Statistics> stat = os.telemetry().meters().statistics("cpu_util", period);
                for (Statistics s : stat) {

                }
            }
        } catch (InterruptedException e) {
            System.exit(1);
        }
    }
}
