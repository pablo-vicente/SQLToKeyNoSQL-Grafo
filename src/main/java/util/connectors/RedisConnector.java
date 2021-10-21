package util.connectors;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;
import com.lisa.sqltokeynosql.architecture.Connector;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
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
    public void connect(String nameDB) {
        pool = new JedisPool(new JedisPoolConfig(), "localhost");
        db = nameDB;
        jedis = pool.getResource();
    }

    @Override
    public void put(String table, String key, LinkedList<String> cols, ArrayList<String> values) {
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
    public void delete(String t, String k) {
        jedis.del(db+"::"+t+"::"+k);
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
        _new.put("_key", key);
        
        return _new;
    }
    
    @Override
    public String toString() {
        return "Redis";
    }

}
