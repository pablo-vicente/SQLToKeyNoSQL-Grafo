package com.lisa.sqltokeynosql.util.connectors;

import com.lisa.sqltokeynosql.architecture.Connector;
import com.mongodb.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.schema.Column;
import com.lisa.sqltokeynosql.util.operations.*;
import com.lisa.sqltokeynosql.util.sql.Table;

import java.util.*;

/**
 * @author geomar
 */
public class MongoConnector extends Connector
{

    public static String UserDefault = "root";
    public static String PasswordDefault = "root";
    public static String UriDefault = "localhost:27017";

    private final MongoClient mongoClient;
    public DB mongoDatabase;

    public MongoConnector()
    {
        var connection = GetConnectionString(UserDefault, PasswordDefault, UriDefault);
        mongoClient = new MongoClient(new MongoClientURI(connection));
    }

    public MongoConnector(String user, String password, String hostAndPort)
    {
        var connection = GetConnectionString(user, password, hostAndPort);
        mongoClient = new MongoClient(new MongoClientURI(connection));
    }

    private String GetConnectionString(String user, String password, String hostAndPort)
    {
        return "mongodb://" + user + ":" + password + "@" + hostAndPort;
    }

    @Override
    public void connect(String nameDB) {
        mongoDatabase = mongoClient.getDB(nameDB);
    }

    public MongoClient getMongoClient()
    {
        return mongoClient;
    }

    @Override
    public void put(Table table, List<String> cols, Map<String, List<String>> dados)
    {
        for (Map.Entry<String, List<String>> stringListEntry : dados.entrySet())
        {
            var key = stringListEntry.getKey();
            var values = stringListEntry.getValue();
            BasicDBObject c = new BasicDBObject("_id", key);
            for (int i = 0; i < cols.size(); i++) {
                c.append(cols.get(i), values.get(i));
            }
            mongoDatabase.getCollection(table.getName()).insert(c);
        }

    }

    @Override
    public void delete(String table, List<String> keys)
    {
        for (String key : keys)
        {
            mongoDatabase.getCollection(table).findAndRemove(new BasicDBObject("_id", key));
        }
    }

    @Override
    public HashMap<String, String> get(int n, String table, final String key) {
        HashMap<String, String> result = null;
        DBObject cursor = mongoDatabase.getCollection(table).findOne(key);
        if (cursor != null) {
            HashMap h = new HashMap<>(cursor.toMap());
            h.put("_key", h);
            result = h;
        }
        return result;
    }

    public ArrayList<HashMap<String, String>> getN(int n, String t, ArrayList<String> keys) {
        ArrayList<HashMap<String, String>> result = new ArrayList();
        DBCursor cursor = mongoDatabase.getCollection(t).find(new BasicDBObject("_id", keys.toArray()));
        while (cursor.hasNext()) {
            DBObject obj = cursor.next();
            HashMap hash = new HashMap<>(obj.toMap());
            hash.put("_key", obj.get("_id"));
            result.add(hash);
        }
        return result;
    }

    @Override
    public ArrayList getN(int n, String t, ArrayList<String> keys, Stack<Object> filters, LinkedList<String> cols) {
        ArrayList<String[]> result = new ArrayList<>();
        DBCursor cursor;

        if (filters == null)
            cursor = mongoDatabase.getCollection(t).find(null, columns(cols));
        else {
            filters = this.resumeFilter(filters, t);
            DBObject query = this.filter(filters);
            cursor = mongoDatabase.getCollection(t).find(query, columns(cols));
        }
        cursor.batchSize(5000000);
        while (cursor.hasNext()) {

            DBObject obj = cursor.next();
            HashMap hash = new HashMap<>(obj.toMap());

            String[] tuple = new String[cols.size()];
            for (int i = 0; i < cols.size(); i++) {
                tuple[i] = String.valueOf(hash.get(cols.get(i)));
            }
            result.add(tuple);

        }


        return result;
    }


    @Override
    public String toString() {
        return "MongoDB";
    }

    private DBObject columns(LinkedList<String> cols) {
        BasicDBObject result = new BasicDBObject();
        cols.forEach(col -> result.put(col, 1));
        return result;
    }

    private DBObject filter(Stack<Object> filters) {
        if (filters == null) {
            return null;
        }

        BasicDBObject result = new BasicDBObject();
        Object statement = filters.pop();
        if (statement instanceof AndExpression) {
            List<DBObject> obj = new ArrayList<>();
            obj.add(filter(filters));
            obj.add(filter(filters));
            result.put("$and", obj);
        } else {
            Object val = filters.pop().toString();
            String col = ((Column) filters.pop()).getColumnName();
            if (statement instanceof Equal)
                result.put(col, val);
            else if (statement instanceof Greater)
                result.put(col, new BasicDBObject("$gt", val));
            else if (statement instanceof Minor)
                result.put(col, new BasicDBObject("$lt", val));
            else if (statement instanceof GreaterEqual)
                result.put(col, new BasicDBObject("$gte", val));
            else if (statement instanceof MinorEqual)
                result.put(col, new BasicDBObject("$lte", val));
        }
        return result;
    }

    private Stack<Object> resumeFilter(Stack<Object> filters, String table) {
        Stack<Object> resumed_filters = null;
        Object t = filters.pop();

        if (t instanceof Operator) {
            Operator o = (Operator) t;
            //Object val = 
            Object val = filters.pop();
            Column tab = (Column) filters.pop();
            if (tab.getTable().getName() == null || tab.getTable().getName().equals(table)) {
                if (filters.empty()) {
                    resumed_filters = new Stack();
                } else {
                    resumed_filters = resumeFilter(filters, table);
                    if (resumed_filters == null)
                        new Stack();
                }
                resumed_filters.add(tab);
                resumed_filters.add(val);
                resumed_filters.add(o);
            }

        }

        return resumed_filters;
    }


}
