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
import java.util.List;

/**
 *
 * @author geomar
 */
public class MongoConnector extends Connector {

    private MongoClient mongoClient;
    DB db;

    public MongoConnector() {
        mongoClient = new MongoClient();
    }

    @Override
    public void connect(String nbd) {
        db = mongoClient.getDB(nbd);
    }

    @Override
    public void put(String table, String key, ArrayList<String> cols, ArrayList<String> values) {
        BasicDBObject c = new BasicDBObject("_id", key);
        for (int i = 0; i < cols.size(); i++) {
            c.append(cols.get(i), values.get(i));
        }
        ((DBCollection) db.getCollection(table)).insert(c);
    }

    @Override
    public void delete() {
    }

    @Override
    public HashMap<String, String> get(int n, String table, final String key) {
        HashMap<String, String> result = null;
        DBObject cursor = db.getCollection(table).findOne(key);
        if (cursor != null){
                result = new HashMap<>(cursor.toMap());    
        }
        return result;
    }

    @Override
    public String toString() {
        return "MongoDB";
    }
    
    
}
