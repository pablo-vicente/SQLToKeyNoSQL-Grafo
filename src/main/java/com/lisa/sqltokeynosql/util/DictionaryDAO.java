package com.lisa.sqltokeynosql.util;

import com.lisa.sqltokeynosql.architecture.Connector;
import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import com.lisa.sqltokeynosql.util.connectors.*;
import com.lisa.sqltokeynosql.util.sql.ForeignKey;
import com.lisa.sqltokeynosql.util.sql.Table;

import java.util.*;

/**
 * @author geomar
 */
public class DictionaryDAO {

    public static void storeDictionary(Dictionary dic) {
        MongoClient mongoClient = new MongoClient(new MongoClientURI("mongodb://root:root@localhost:27017"));
        MongoDatabase db = mongoClient.getDatabase("_dictionary");
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
            aux.append("connector", n.getConnector().toString());
            targets.append(n.getAlias(), aux);
        }
        Document auxTable, tables;
        for (BDR bd : dic.getRdbms()) {
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
        db.getCollection("_dictionary").insertOne(dictionary.append("targets", targets).append("current_db", getName(dic)).append("bdrs", bdrs));
        mongoClient.close();
    }

    private static String getName(Dictionary dic) {
        if (dic != null && dic.getCurrentDb() != null){
            return dic.getCurrentDb().getName();
        }

        return null;
    }

    public static Optional<Dictionary> loadDictionary() {
        MongoClient mongoClient = new MongoClient(new MongoClientURI("mongodb://root:root@localhost:27017"));
        MongoDatabase db = mongoClient.getDatabase("_dictionary");
        MongoCollection<Document> cdic = db.getCollection("_dictionary");
        if (cdic.count() < 1) {
            System.out.println("Dictionary not found!!");
            mongoClient.close();
            return Optional.empty();
        }
        Dictionary _new = new Dictionary();
        db.getCollection("_dictionary").find().forEach((Block<Document>) document -> {
            System.out.println("Dictionary was founded!!");
            Collection<Object> targets = ((Document) document.get("targets")).values();
            Iterator<Object> iteratorTarget = targets.iterator();
            Document aux;
            while (iteratorTarget.hasNext()) {
                aux = (Document) iteratorTarget.next();

                String alias = aux.getString("alias");
                String user = aux.getString("user");
                String psw = aux.getString("psw");
                String url = aux.getString("url");
                var connector =  aux.getString("connector");

                NoSQL n = new NoSQL(alias, user, psw, url, connector);
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
                            t.setFks(new ArrayList<>());
                            Iterator fkIterator = fks.values().iterator();
                            while (fkIterator.hasNext()) {
                                Document fkD = (Document) fkIterator.next();
                                t.getFks().add(new ForeignKey(fkD.getString("att"), fkD.getString("attR"), fkD.getString("tableR")));
                            }

                        } else {
                            t.setFks(new ArrayList<>());
                        }
                        bd.getTables().add(t);
                    }
                }
                _new.getRdbms().add(bd);
            }
            _new.setCurrentDb(document.getString("current_db"));
        });

        mongoClient.close();
        return Optional.ofNullable(_new);
    }

}
