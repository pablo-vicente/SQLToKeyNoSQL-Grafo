/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package util;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Locale;

import static java.util.Arrays.asList;
import java.util.List;


/**
 *
 * @author geomar
 */
public class MongoConnector extends Connector {
    private MongoClient mongoClient;
    MongoDatabase db;
    
    public MongoConnector(){
         mongoClient = new MongoClient();
    }
    
    @Override
    public void connect(String nbd){
        db = mongoClient.getDatabase(nbd);
    }
    
    @Override
    public void put (String table, String key, ArrayList<String>cols, ArrayList<String>values){
        if (db.getCollection(table) == null){
            db.createCollection(table);
        }
        Document current = new Document();
        for(int i=0;i<cols.size();i++)
            current.append(cols.get(i), values.get(i));
        db.getCollection(table).insertOne(new Document(key,current));
            
    }
    
    @Override
    public void delete(){}
    
    @Override
    public void get(){}
}
