/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package util;

import com.lisa.sqltokeynosql.architecture.Connector;
import com.mongodb.BasicDBObject;
import com.mongodb.Block;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Locale;

import static java.util.Arrays.asList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Stack;

/**
 *
 * @author geomar
 */
public class MongoConnector extends Connector {

    private MongoClient mongoClient;
    DB db;
    MongoDatabase db2;

    public MongoConnector() {
        mongoClient = new MongoClient();
    }

    @Override
    public void connect(String nbd) {
        db = mongoClient.getDB(nbd);
        db2 = mongoClient.getDatabase(nbd);
    }

    @Override
    public void put(String table, String key, LinkedList<String> cols, ArrayList<String> values) {
        BasicDBObject c = new BasicDBObject("_id", key);
        for (int i = 0; i < cols.size(); i++) {
            c.append(cols.get(i), values.get(i));
        }
        ((DBCollection) db.getCollection(table)).insert(c);
    }

    @Override
    public void delete(String table, String key) {
        DBObject cursor = db.getCollection(table).findAndRemove(new BasicDBObject("_id", key));
    }

    @Override
    public HashMap<String, String> get(int n, String table, final String key) {
        HashMap<String, String> result = null;
        DBObject cursor = db.getCollection(table).findOne(key);
        if (cursor != null){
                HashMap h = new HashMap<>(cursor.toMap());
                h.put("_key", h);
                result = h;    
        }
        return result;
    }

     public ArrayList<HashMap<String, String>> getN(int n, String t,ArrayList<String> keys){
        ArrayList<HashMap<String, String>> result = new ArrayList();
        DBCursor cursor = db.getCollection(t).find(new BasicDBObject("_id", keys.toArray()));
        while(cursor.hasNext()){
            DBObject obj = cursor.next();
            HashMap hash = new HashMap<>(obj.toMap());
            hash.put("_key", obj.get("_id"));
            result.add(hash);
        }
        return result;
    }

//    @Override
//    public ArrayList<HashMap<String, String>> getN(int n, String t, ArrayList<String> keys, Stack<Object> filters) {
//        ArrayList<HashMap<String, String>> result = new ArrayList();
//        DBCursor cursor = db.getCollection(t).find(new BasicDBObject("_id", keys.toArray()));
//        while(cursor.hasNext()){
//            DBObject obj = cursor.next();
//            HashMap hash = new HashMap<>(obj.toMap());
//            hash.put("_key", obj.get("_id"));
//            if (applyFilterR(filters, hash)) {
//                result.add(hash);
//            }
//        }
//        return result;
//    }
     
    @Override
    public ArrayList getN(int n, String t, ArrayList<String> keys, Stack<Object> filters, LinkedList<String> cols) {
        ArrayList<String[]> result = new ArrayList<String[]>();
        FindIterable<Document> iterable = db2.getCollection(t).find();
        iterable.forEach(new Block<Document>() {
            @Override
            public void apply(final Document document) {
                HashMap hash = new HashMap<>(document);
                //hash.put("_key", document.get("_id"));
                if (applyFilterR((filters != null ? (Stack) filters.clone() : null), hash)) {
                    String[] tuple = new String[cols.size()];
                    for (int i=0;i<cols.size();i++){
                        tuple[i] = String.valueOf(hash.get(cols.get(i)));
                    }
                    result.add(tuple);
                }
            }
        });

        return result;
    }
     
    

    @Override
    public String toString() {
        return "MongoDB";
    }
    
    
}
