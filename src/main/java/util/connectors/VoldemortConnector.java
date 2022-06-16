package util.connectors;

import com.lisa.sqltokeynosql.architecture.Connector;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import org.json.JSONObject;
import util.Dictionary;
import util.SQL.Table;
import voldemort.client.ClientConfig;
import voldemort.client.SocketStoreClientFactory;
import voldemort.client.StoreClient;
import voldemort.client.StoreClientFactory;

/**
 *
 * @author geomar
 */
public class VoldemortConnector extends Connector {

    StoreClientFactory factory;
    StoreClient<String, String> client;

    public VoldemortConnector() {
        this.factory = new SocketStoreClientFactory(new ClientConfig().setBootstrapUrls("tcp://localhost:6666"));

    }

    @Override
    public void connect(String nbd) {
        client = factory.getStoreClient(nbd);
    }

    @Override
    public void put(Dictionary dictionary, Table table, String key, LinkedList<String> cols, ArrayList<String> values) {
        HashMap<String, String> current = new HashMap<>();
        for (int i = 0; i < cols.size(); i++) {
            current.put(cols.get(i), values.get(i));
        }
        JSONObject json = new JSONObject(current);
        client.put(table.getName() + "." + key, current.toString());
    }

    @Override
    public void delete(String t, String k) {
        
    }

    @Override
    public HashMap<String, String> get(int n, String t, String key) {
        String s = (String) client.getValue(t + "." + key);
        HashMap<String, String> _new = new HashMap<>();
        for (String s1 : s.split(",")) {
            String[] s2 = s1.split(":");
            _new.put(s2[0], s2[1]);
        }
        return _new;
    }

}
