package util;

import com.google.gson.Gson;
import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import org.bson.Document;

/**
 *
 * @author geomar
 */
public class DictionaryDAO {

    public static void storeDictionary(Dictionary dic) {
        MongoClient mongoClient = new MongoClient();
        MongoDatabase db = mongoClient.getDatabase("test");
        Document dictionary = new Document("_id", "_dictionary");
        Document bdrs = new Document();
        Document targets = new Document();
        Document aux;
        for (NoSQL n : dic.getTargets()) {
            aux = new Document();
            aux.append("alias", n.getAlias());
            aux.append("url", n.getUrl());
            aux.append("user", n.getUser());
            aux.append("psw", n.getPassword());
            int con = 1;
            if (n.getConection() instanceof MongoConnector) {
                con = 1;
            } else if (n.getConection() instanceof Cassandra2Connector) {
                con = 2;
            } else if (n.getConection() instanceof CassandraConnector) {
                con = 3;
            } else if (n.getConection() instanceof RedisConnector) {
                con = 4;
            }
            aux.append("connector", con);
            targets.append(n.getAlias(), aux);
        }
        Document auxTable, tables;
        for (BDR bd : dic.getBdrs()) {
            aux = new Document();
            aux.append("name", bd.getName());
            tables = new Document();
            for (Table t : bd.getTables()) {
                auxTable = new Document();
                auxTable.append("name", t.getName());
                auxTable.append("target", t.getTargetDB().getAlias());
                auxTable.append("fk", t.getFks().toString().replace("[", "").replace("]", ""));
                auxTable.append("pk", t.getPks().toString().replace("[", "").replace("]", ""));
                auxTable.append("att", t.getAttributes().toString().replace("[", "").replace("]", ""));
                auxTable.append("key", t.getKeys().toString().replace("[", "").replace("]", ""));
                tables.append(t.getName(), auxTable);
            }
            bdrs.append(bd.getName(), aux).append("tables", tables);
        }
        db.getCollection("_dictionary").insertOne(dictionary.append("targets", targets).append("current_db", dic.getCurrent_db()).append("bdrs", bdrs));
        mongoClient.close();
    }

    public static Dictionary loadDictionary() {
        MongoClient mongoClient = new MongoClient();
        MongoDatabase db = mongoClient.getDatabase("test");
        MongoCollection<Document> cdic = db.getCollection("_dictionary");
        if (cdic.count() < 1) {
            System.out.println("Dictionary not found!!");
            mongoClient.close();
            return null;
        }
        Dictionary _new = new Dictionary();
        cdic.find().forEach(new Block<Document>() {
            @Override
            public void apply(final Document document) {
                if (document.get("_id").equals("_dictionary")) {
                    System.out.println("Dictionary was founded!!");
                    _new.setCurrent_db(document.getString("current_db"));
                    Collection<Object> targets = ((Document) document.get("targets")).values();
                    Iterator<Object> iteratorTarget = targets.iterator();
                    Document aux;
                    while (iteratorTarget.hasNext()) {
                        aux = (Document) iteratorTarget.next();
                        NoSQL n = new NoSQL(aux.getString("alias"), aux.getString("user"), aux.getString("psw"), aux.getString("url"));
                        switch (aux.getInteger("connector")) {
                            case 1: {
                                n.setConection(new MongoConnector());
                            }
                            case 2: {
                                n.setConection(new Cassandra2Connector());
                            }
                            case 3: {
                                n.setConection(new CassandraConnector());
                            }
                            case 4: {
                                n.setConection(new RedisConnector());
                            }
                            default: {
                                n.setConection(new MongoConnector());
                            }
                        }
                        _new.getTargets().add(n);
                    }

                    System.out.println("loading... RDBs!!");
                    Collection<Object> bdrs = ((Document) document.get("bdrs")).values();
                    iteratorTarget = targets.iterator();
                    BDR bd;
                    while (iteratorTarget.hasNext()) {
                        aux = (Document) iteratorTarget.next();
                        bd = new BDR();
                        bd.setName(aux.getString("name"));
                        _new.getBdrs().add(bd);
                    }
                }

            }
        });

        mongoClient.close();
        return _new;
    }

}
