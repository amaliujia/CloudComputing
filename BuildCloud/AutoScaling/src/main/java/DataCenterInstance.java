import javafx.scene.chart.XYChart;
import org.openstack4j.model.compute.Server;

import java.util.List;

/**
 * Created by amaliujia on 16-2-8.
 */
public class DataCenterInstance {;
    public final Server server;

    public DataCenterInstance(Server server) {
        this.server = server;
    }

    public String getName() {
        return server.getName();
    }

    public String getUrl() {
        return "http://" + server.getAccessIPv4();
    }

    public String getIP() {
        return server.getAccessIPv4();
    }

    public String getServerID() {
        return server.getId();
    }

    public String toString() {
        return server.getId() + "\t" + server.getName() + "\t" + server.getAccessIPv4() + "\t" + server.getImageId()
                + "\t" + server.getFlavorId() + "\n";
    }
}
