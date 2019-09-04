/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package util.connectors;

import com.lisa.sqltokeynosql.architecture.Connector;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Stack;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.schema.Column;
import util.operations.Equal;
import util.operations.Greater;
import util.operations.GreaterEqual;
import util.operations.Minor;
import util.operations.MinorEqual;
import util.operations.Operator;

/**
 *
 * @author geomar
 */
public class MongoConnector extends Connector {

    private final MongoClient mongoClient;
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


     
//    @Override
//    public ArrayList getN(int n, String t, ArrayList<String> keys, Stack<Object> filters, LinkedList<String> cols) {
//        ArrayList<String[]> result = new ArrayList<String[]>();
//       // FindIterable<Document> iterable = db2.getCollection(t).find();
//        
//        DBCursor cursor = db.getCollection(t).find();
//        cursor.batchSize(5000000);
//        while(cursor.hasNext()){
//            DBObject obj = cursor.next();
//            HashMap hash = new HashMap<>(obj.toMap());
//            if (applyFilterR((filters != null ? (Stack) filters.clone() : null), hash)) {
//                    String[] tuple = new String[cols.size()];
//                    for (int i=0;i<cols.size();i++){
//                        tuple[i] = String.valueOf(hash.get(cols.get(i)));
//                    }
//                    result.add(tuple);
//                }
//        }
//
//        return result;
//    }
//    
    @Override
    public ArrayList getN(int n, String t, ArrayList<String> keys, Stack<Object> filters, LinkedList<String> cols) {
        ArrayList<String[]> result = new ArrayList<>();
         DBCursor cursor;
        
        if (filters == null)
            cursor = db.getCollection(t).find(null, columns(cols));
        else{
            filters = this.resumeFilter(filters,t);
            DBObject query = this.filter(filters);
            cursor = db.getCollection(t).find(query,columns(cols));
        }
        cursor.batchSize(5000000);
       while(cursor.hasNext()){
            
                DBObject obj = cursor.next();
            HashMap hash = new HashMap<>(obj.toMap());
//            
                    String[] tuple = new String[cols.size()];
                    for (int i=0;i<cols.size();i++){
                        tuple[i] = String.valueOf(hash.get(cols.get(i)));
                    }
                    result.add(tuple);
                //}
            }
        

        return result;
    }
     
    

    @Override
    public String toString() {
        return "MongoDB";
    }
    
    private DBObject columns(LinkedList<String> cols){
        BasicDBObject result = new BasicDBObject();
        cols.stream().forEach((col) -> {
            result.put(col, 1);
        });
        return result;
    }
    
    private DBObject filter(Stack filters){
        if (filters == null) {
            return null;
        }
        
        BasicDBObject result = new BasicDBObject();
        //BSON result = new Document();
        Object o = filters.pop();
        if (o instanceof AndExpression) {
            List<DBObject> obj = new ArrayList<DBObject>();
            obj.add(filter(filters));
            obj.add(filter(filters));
            result.put("$and", obj);
            
          //  result = (result && applyFilterR(filters, tuple));
    //    } else if (o instanceof OrExpression) {
  //          result = applyFilterR(filters, tuple);
   //         result = (result || applyFilterR(filters, tuple));
        } else {
            String col = null;
            Object val = null;
            Operator op = null;
            op = ((Operator) o);
            val = filters.pop().toString();
            col = ((Column) filters.pop()).getColumnName();
            if (o instanceof Equal)
                result.put(col, val);
            else if (o instanceof Greater)
                result.put(col, new BasicDBObject("$gt",val));
            else if (o instanceof Minor)
                result.put(col, new BasicDBObject("$lt",val));
            else if (o instanceof GreaterEqual)
                result.put(col, new BasicDBObject("$gte",val));
            else if (o instanceof MinorEqual)
                result.put(col, new BasicDBObject("$lte",val));
        }
        return  result;
    }

    private Stack<Object> resumeFilter(Stack<Object> filters, String table) {
        Stack<Object> resumed_filters = null;
        Object t = filters.pop();
        
        //if (t instanceof AndExpression) {
         //   resumed_filters.a
        //}else 
            if (t instanceof Operator){
            Operator o = (Operator)t;
            //Object val = 
            Object val = filters.pop();
            Column tab = (Column) filters.pop();
            if ( tab.getTable().getName() == null || tab.getTable().getName().equals(table)){
                if (filters.empty()){
                    resumed_filters = new Stack();
                }else{
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
