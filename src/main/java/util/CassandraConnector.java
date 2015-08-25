package util;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.lisa.sqltokeynosql.architecture.Connector;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

/**
 *
 * @author geomar
 */
public class CassandraConnector extends Connector {

    Cluster cluster;
    Session session;

    public CassandraConnector() {
        cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
    }

    @Override
    public void connect(String nbd) {
        session = cluster.connect(nbd);
    }

    @Override
    public void put(String table, String key, ArrayList<String> cols, ArrayList<String> values) {
        String cql = "INSERT INTO " + table + " ";
        String c = "(key, ", v = "('" + key + "', ";
        for (int i = 0; i < cols.size(); i++) {
            c += cols.get(i);
            v += values.get(i);

            if (i != cols.size() - 1) {
                c += ", ";
                v += ", ";
            } else {
                c += ")";
                v += ")";
            }
        }
        session.execute(cql + c + " VALUES " + v);
    }

    @Override
    public void delete() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public HashMap<String, String> get(int n, String t, String key) {
        HashMap<String, String> current = new HashMap<>();
        ResultSet results = session.execute("SELECT * FROM "+t+" WHERE key='"+key+"'");
        Row row = results.one();
         Iterator<ColumnDefinitions.Definition> i =  row.getColumnDefinitions().iterator();
         while (i.hasNext()){
             ColumnDefinitions.Definition def = i.next();
             
             if (def.getType().asJavaClass() == Integer.class)
                current.put(def.getName(),String.valueOf(row.getInt(def.getName())));
             else if (def.getType().asJavaClass() == Float.class)
                current.put(def.getName(),String.valueOf(row.getFloat(def.getName())));
             else if (def.getType().asJavaClass() == Double.class)
                current.put(def.getName(),String.valueOf(row.getDouble(def.getName())));
             else 
                current.put(def.getName(),row.getString(def.getName())); 
         }
        return current;
    }

}
