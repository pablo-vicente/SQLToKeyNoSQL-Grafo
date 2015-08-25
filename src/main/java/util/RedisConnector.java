package util;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;
import com.lisa.sqltokeynosql.architecture.Connector;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
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
        jedis = pool.getResource();
    }

    @Override
    public void put(String table, String key, ArrayList<String> cols, ArrayList<String> values) {
        if (jedis != null){
            HashMap<String, String> current = new HashMap<>();
            for (int i = 0; i < cols.size(); i++) {
                current.put(cols.get(i), values.get(i));
            }
            String rkey = db+"::"+table+"::"+key;
            jedis.set(rkey, current.toString());
            //jedis.close();
        }
        
    }

    @Override
    public void delete() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public HashMap<String, String> get(int n, String t, String key) {
       // jedis = pool.getResource();
         String s = (String) jedis.get(this.db+"::"+t +"::" + key);
        HashMap<String, String> _new = new HashMap<>();
       // JsonObject  obj = new JsonParser().parse(s).getAsJsonObject();
        Map<String, String> retMap = new Gson().fromJson(s, new TypeToken<HashMap<String, String>>() {}.getType());
        _new = (HashMap<String, String>) retMap;
       // jedis.close();
        
        
        return _new;
    }

}
