package util;

import com.lisa.sqltokeynosql.architecture.Connector;
import util.SQL.ForeignKey;
import util.SQL.Table;
import util.connectors.*;
import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import org.bson.Document;

/**
 *
 * @author geomar
 */
public class DictionaryDAO {

    public static void storeDictionary(Dictionary dic) {
        MongoClient mongoClient = MongoConnector.GetDefaultInstace();
        MongoDatabase db = mongoClient.getDatabase("_dictionary");
        Document dictionary = new Document("_id", "_dictionary");
        //Document dictionary = new Document();
        Document bdrs = new Document();
        Document targets = new Document();
        Document aux;
        for (NoSQL noSQL : dic.getTargets()) {
            aux = new Document();
            aux.append("alias", noSQL.getAlias());
            aux.append("url", noSQL.getUrl());
            aux.append("user", noSQL.getUser());
            aux.append("psw", noSQL.getPassword());

            Connector connector = noSQL.getConection();
            String conncetoName = connector.getClass().getSimpleName();
            aux.append("connector", conncetoName);
            targets.append(noSQL.getAlias(), aux);
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

    private static Connector GetConnector(String connectorName)
    {
        if(connectorName.equalsIgnoreCase(MongoConnector.class.getSimpleName()))
            return new MongoConnector();

        if(connectorName.equalsIgnoreCase(Cassandra2Connector.class.getSimpleName()))
            return new Cassandra2Connector();

        if(connectorName.equalsIgnoreCase(CassandraConnector.class.getSimpleName()))
            return new CassandraConnector();

        if(connectorName.equalsIgnoreCase(RedisConnector.class.getSimpleName()))
            return new RedisConnector();

        if(connectorName.equalsIgnoreCase(SimpleDBConnector.class.getSimpleName()))
            return new SimpleDBConnector();

        if(connectorName.equalsIgnoreCase(Neo4jConnector.class.getSimpleName()))
            return new Neo4jConnector();

        throw new UnsupportedOperationException("Connector not declared!!!!");
    }

    public static Dictionary loadDictionary() {
        MongoClient mongoClient = MongoConnector.GetDefaultInstace();
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

                    String alias = aux.getString("alias");
                    String user = aux.getString("user");
                    String psw = aux.getString("psw");
                    String url = aux.getString("url");


                    String connetion = aux.getString("connector");

                    Connector connector = GetConnector(connetion);
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
