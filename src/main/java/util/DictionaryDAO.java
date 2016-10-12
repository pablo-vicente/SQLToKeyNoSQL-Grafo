package util;

import com.google.gson.Gson;
import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import javax.swing.text.StyledEditorKit;
import org.bson.Document;

/**
 *
 * @author geomar
 */
public class DictionaryDAO {

    public static void storeDictionary(Dictionary dic) {
        MongoClient mongoClient = new MongoClient();
        MongoDatabase db = mongoClient.getDatabase("_dictionary");
        Document dictionary = new Document("_id", "_dictionary");
        //Document dictionary = new Document();
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
            } else if (n.getConection() instanceof SimpleDBConnector) {
                con = 5;
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
                if (t.getFks() != null || !t.getFks().isEmpty()) {
                    Document fk, fks = new Document();
                    for (ForeignKey f : t.getFks()) {
                        fk = new Document();
                        fk.append("att", f.getAtt());
                        fk.append("attR", f.getrAtt());
                        fk.append("tableR", f.getrTable());
                        fks.append(f.getAtt() + "_fk", fk);
                    }
                    auxTable.append("fk", fks);
                } else {
                    //auxTable.append("fk", "");
                }
                auxTable.append("pk", t.getPks().toString().replace("[", "").replace("]", ""));
                auxTable.append("att", t.getAttributes().toString().replace("[", "").replace("]", ""));
                auxTable.append("key", t.getKeys().toString().replace("[", "").replace("]", ""));
                tables.append(t.getName(), auxTable);
            }
            aux.append("tables", tables);
            bdrs.append(bd.getName(), aux);
        }
        db.getCollection("_dictionary").drop();
        db.getCollection("_dictionary").insertOne(dictionary.append("targets", targets).append("current_db", dic.getCurrent_db().getName()).append("bdrs", bdrs));
        mongoClient.close();
    }

    public static Dictionary loadDictionary() {
        MongoClient mongoClient = new MongoClient();
        MongoDatabase db = mongoClient.getDatabase("_dictionary");
        MongoCollection<Document> cdic = db.getCollection("_dictionary");
        if (cdic.count() < 1) {
            System.out.println("Dictionary not found!!");
            mongoClient.close();
            return null;
        }
        Dictionary _new = new Dictionary();
        db.getCollection("_dictionary").find().forEach(new Block<Document>() {
            @Override
            public void apply(final Document document) {
                //if (document.get("_id").equals("_dictionary")) {
                System.out.println("Dictionary was founded!!");
                Collection<Object> targets = ((Document) document.get("targets")).values();
                Iterator<Object> iteratorTarget = targets.iterator();
                Document aux;
                while (iteratorTarget.hasNext()) {
                    aux = (Document) iteratorTarget.next();
                    NoSQL n = new NoSQL(aux.getString("alias"), aux.getString("user"), aux.getString("psw"), aux.getString("url"));
                    switch (aux.getInteger("connector")) {
                        case 1: {
                            n.setConection(new MongoConnector());
                            break;
                        }
                        case 2: {
                            n.setConection(new Cassandra2Connector());
                            break;
                        }
                        case 3: {
                            n.setConection(new CassandraConnector());
                            break;
                        }
                        case 4: {
                            n.setConection(new RedisConnector());
                            break;
                        }
                        case 5: {
                            n.setConection(new SimpleDBConnector());
                            break;
                        }
                        default: {
                            n.setConection(new MongoConnector());
                        }
                    }
                    _new.getTargets().add(n);
                }

                System.out.println("loading... RDBs!!");
                Collection<Object> bdrs = ((Document) document.get("bdrs")).values();
                Iterator iteratorRBD = bdrs.iterator();
                BDR bd;
                Document tableD;
                while (iteratorRBD.hasNext()) {
                    aux = (Document) iteratorRBD.next();
                    bd = new BDR();
                    bd.setName(aux.getString("name"));
                    if (aux.get("tables") != null && !((Document) aux.get("tables")).values().isEmpty()) {
                        Iterator tableIterator = ((Document) aux.get("tables")).values().iterator();
                        while (tableIterator.hasNext()) {
                            tableD = (Document) tableIterator.next();
                            Table t = new Table(tableD.getString("name"), _new.getTarget(tableD.getString("target")), null, null, null);
                            t.setAttributes(new LinkedList<>(Arrays.asList(tableD.getString("att").replace(" ", "").split(","))));
                            if (tableD.getString("key").isEmpty()) {
                                t.setKeys(new ArrayList<>());
                            } else {
                                t.setKeys(new ArrayList<>(Arrays.asList(tableD.getString("key").replace(" ", "").split(","))));
                            }
                            if (tableD.getString("pk").isEmpty()) {
                                t.setPks(new ArrayList<>());
                            } else {
                                t.setPks(new ArrayList<>(Arrays.asList(tableD.getString("pk").replace(" ", "").split(","))));
                            }
                            Document fks = (Document) tableD.get("fk");
                            if (fks != null) {
                                t.setFks(new ArrayList<ForeignKey>());
                                Iterator fkIterator = fks.values().iterator();
                                while (fkIterator.hasNext()) {
                                    Document fkD = (Document) fkIterator.next();
                                    t.getFks().add(new ForeignKey(fkD.getString("att"), fkD.getString("attR"), fkD.getString("tableR")));
                                }

                            } else {
                                t.setFks(new ArrayList<ForeignKey>());
                            }
                            bd.getTables().add(t);
                        }
                    }
                    _new.getBdrs().add(bd);
                }
                _new.setCurrent_db(document.getString("current_db"));

            }

            //}
        });

        mongoClient.close();
        return _new;
    }

}
