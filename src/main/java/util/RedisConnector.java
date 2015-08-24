package util;

import com.lisa.sqltokeynosql.architecture.Connector;
import java.util.ArrayList;
import java.util.HashMap;
import org.json.JSONObject;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 *
 * @author geomar
 */
public class RedisConnector extends Connector{
    Jedis jedis;
    JedisPool pool;
    String db;

    @Override
    public void connect(String nbd) {
        pool = new JedisPool(new JedisPoolConfig(), "localhost");
        db = nbd;
    }

    @Override
    public void put(String table, String key, ArrayList<String> cols, ArrayList<String> values) {
        jedis = pool.getResource();
        if (jedis != null){
            HashMap<String, String> current = new HashMap<>();
            for (int i = 0; i < cols.size(); i++) {
                current.put(cols.get(i), values.get(i));
            }
            String rkey = db+"::"+table+"::"+key;
            jedis.set(rkey, current.toString());
            jedis.close();
        }
        
    }

    @Override
    public void delete() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public HashMap<String, String> get(int n, String t, String key) {
         String s = (String) jedis.get(this.db+"::"+t +"::" + key);
        HashMap<String, String> _new = new HashMap<>();
        s = s.substring(1, s.length()-1);
        for (String s1 : s.split(",")) {
            String[] s2 = s1.split("=");
            _new.put(s2[0].trim(), s2[1].trim());
        }
        return _new;
    }

}
