package com.lisa.sqltokeynosql.util.connectors;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.lisa.sqltokeynosql.architecture.Connector;
import com.lisa.sqltokeynosql.util.sql.Table;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.*;

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
    public void put(Table table, List<String> cols, Map<String, List<String>> dados)
    {
        for (Map.Entry<String, List<String>> stringListEntry : dados.entrySet())
        {
            var key = stringListEntry.getKey();
            var values = stringListEntry.getValue();
            if (jedis != null){
                HashMap<String, String> current = new HashMap<>();
                for (int i = 0; i < cols.size(); i++) {
                    current.put(cols.get(i), values.get(i));
                }
                String rkey = db+"::"+table.getName()+"::"+key;
                jedis.set(rkey, current.toString());
                //jedis.close();
            }
        }
    }

    @Override
    public void delete(String t, String...keys)
    {
        for (String k : keys)
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
