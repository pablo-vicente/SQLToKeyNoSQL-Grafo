package com.lisa.sqltokeynosql.util.connectors;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.lisa.sqltokeynosql.architecture.Connector;
import com.lisa.sqltokeynosql.util.sql.Table;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

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
    public void connect(String nameDB) {
        session = cluster.connect(nameDB);
    }

    @Override
    public void put(com.lisa.sqltokeynosql.util.Dictionary dictionary, Table table, String key, LinkedList<String> cols, ArrayList<String> values) {
        String cql = "INSERT INTO " + table.getName() + " ";
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
    public void delete(String t, String k) {
        String cql = "DELETE FROM "+t+" WHERE key='"+k+"'";
        session.execute(cql);
    }

    @Override
    public HashMap<String, String> get(int n, String t, String key) {
        HashMap<String, String> current = new HashMap<>();
        ResultSet results = session.execute("SELECT * FROM " + t + " WHERE key='" + key + "'");
        Row row = results.one();
        Iterator<ColumnDefinitions.Definition> i = row.getColumnDefinitions().iterator();
        while (i.hasNext()) {
            ColumnDefinitions.Definition def = i.next();

            if (def.getType().asJavaClass() == Integer.class) {
                current.put(def.getName(), String.valueOf(row.getInt(def.getName())));
            } else if (def.getType().asJavaClass() == Float.class) {
                current.put(def.getName(), String.valueOf(row.getFloat(def.getName())));
            } else if (def.getType().asJavaClass() == Double.class) {
                current.put(def.getName(), String.valueOf(row.getDouble(def.getName())));
            } else {
                current.put(def.getName(), row.getString(def.getName()));
            }
        }
        return current;
    }

    @Override
    public String toString() {
        return "Cassandra";
    }

    public ArrayList<HashMap<String, String>> getN(int n, String t, ArrayList<String> keys) {
        ArrayList<HashMap<String, String>> result = new ArrayList();
        HashMap<String, String> current;
        ResultSet results = session.execute("SELECT * FROM " + t + " limit 1000000");
        List<Row> rows = results.all();
        Iterator<ColumnDefinitions.Definition> i;
        for (Row row : rows) {
            current = new HashMap<>();
            i = row.getColumnDefinitions().iterator();
            while (i.hasNext()) {
                ColumnDefinitions.Definition def = i.next();
                String cName = def.getName();
                if (cName.equals("key")){
                    cName = "_key";
                }
                if (def.getType().asJavaClass() == Integer.class) {
                    current.put(cName, String.valueOf(row.getInt(def.getName())));
                } else if (def.getType().asJavaClass() == Float.class) {
                    current.put(cName, String.valueOf(row.getFloat(def.getName())));
                } else if (def.getType().asJavaClass() == Double.class) {
                    current.put(cName, String.valueOf(row.getDouble(def.getName())));
                } else {
                    current.put(cName, row.getString(def.getName()));
                }
            }
            result.add(current);
        }
        System.out.println("Cassandra Linhas:" + result.size());
        return result;
    }
}
