/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package util.connectors;
import java.util.*;
import com.lisa.sqltokeynosql.architecture.Connector;
import org.neo4j.driver.*;
import org.neo4j.driver.Record;
import org.neo4j.driver.summary.SummaryCounters;
import util.Dictionary;
import util.SQL.ForeignKey;
import util.SQL.Table;

import static org.neo4j.driver.Values.parameters;

/**
 *
 * @author Pablo Vicente infelizmente
 */
public class Neo4jConnector extends Connector implements AutoCloseable
{
    private final Driver driver;
    private static String DbName = "Neo4j";
    private String _nomeBancoDados = "";
    private final String _idColumnName = "id";

    public Neo4jConnector()
    {
        String uri = "bolt://localhost:7687";
        String user = "neo4j";
        String password = "Neo4j";
        driver = GraphDatabase.driver( uri, AuthTokens.basic( user, password ) );
    }

    /**
     * @param nbd
     */
    @Override
    public void connect(String nbd)
    {
        _nomeBancoDados = nbd
                .trim()
                .toLowerCase()
                .replace("-", ".")
                .replace("_", ".");
        try (Session session = driver.session())
        {
            String query = "CREATE DATABASE " + _nomeBancoDados + " IF NOT EXISTS";
            session.run(query);
        }
    }

    /**
     * @param table
     * @param key
     * @param cols
     * @param values
     */
    @Override
    public void put(Dictionary dictionary, Table table, String key, LinkedList<String> cols, ArrayList<String> values)
    {
        try (Session session = driver.session(SessionConfig.forDatabase(_nomeBancoDados)))
        {
            verifyDuplicateId(table, key, session);
            String querysRelationships = verifyRelationships(table, values, session);

            ////////////////////////
            ArrayList<Table> tables = dictionary.getCurrent_db().getTables();
            String queryRelationShipWithtable = "";
            for (int i = 0; i < tables.size(); i++)
            {
                Table tableWithFk = tables.get(i);
                ArrayList<ForeignKey> fks = tableWithFk.getFks();

                for (int i1 = 0; i1 < fks.size(); i1++)
                {
                    ForeignKey foreignKey = fks.get(i1);
                    String tableFk = foreignKey.getrTable();
                    String tableAttributeReferenceFk = foreignKey.getrAtt();
                    String fkAttribute = foreignKey.getAtt();

                    if(!tableFk.equalsIgnoreCase(table.getName()))
                        continue;
                    String nodeShortCutName = "c" + i1;
                    queryRelationShipWithtable +=  "WITH (n)\n" +
                                "MATCH (" + nodeShortCutName + ":" + tableWithFk.getName() + ")\n" +
                                "WHERE " + nodeShortCutName + "." + fkAttribute + " = n." + tableAttributeReferenceFk + "\n" +
                                "CREATE (" + nodeShortCutName + ")-[:" + tableAttributeReferenceFk + "]->(n)\n";
                }
            }

            ////////////////////////


            Map<String, Object> props = getStringObjectMap(key, cols, values);
            String queryInsert =  "CREATE (n:" + table.getName() + " $props)\n" +
                                    querysRelationships + queryRelationShipWithtable;

            Result result =session.run(queryInsert, parameters("props", props));
            SummaryCounters summaryCounters = result.consume().counters();
            int deletedNodes = summaryCounters.nodesCreated() + summaryCounters.relationshipsCreated();
            verifyQueryResult(deletedNodes, queryInsert);
        }
    }
    private String verifyRelationships(Table table, ArrayList<String> values, Session session)
    {
        ArrayList<String> erros = new ArrayList<>();
        ArrayList<ForeignKey> pks = table.getFks();
        String queryRelationShip = "";

        for (int i = 0; i < pks.size(); i++) {
            ForeignKey pk = pks.get(i);

            String tableFk = pk.getrTable();
            String tableAttributeReferenceFk = pk.getrAtt();
            String fkAttribute = pk.getAtt();

            int indexFkAttribute = table.getAttributes().indexOf(fkAttribute);
            String valueFkAttribute = values.get(indexFkAttribute);
            String queryFk = getQueryAttribute(tableFk, tableAttributeReferenceFk, valueFkAttribute);
            List<Record> results = session.run(queryFk).list();

            if(results.size() != 1)
                erros.add("Nao foi possivel criar relacionamento entre atributo '" + fkAttribute + " '" +
                        " com a tabela '" + tableFk + " (" + tableAttributeReferenceFk + ")'" +
                        " para o valor '" + valueFkAttribute + "' inexistente.");
            else
            {
                String nodeShortCutName = "b" + i;
                queryRelationShip +=    "WITH (n)\n" +
                                        "MATCH (" + nodeShortCutName + ":" + tableFk + ")\n" +
                                        "WHERE " + nodeShortCutName + "." + tableAttributeReferenceFk + " = " + valueFkAttribute + "\n" +
                                        "CREATE (n)-[:" + tableAttributeReferenceFk + "]->(" + nodeShortCutName + ")\n";
            }

        }

        for (int i = 0; i < erros.size(); i++)
            System.out.println(erros.get(i));

        if(erros.size() > 0)
            throw new UnsupportedOperationException("Não foi possível criar relacionamentos. Verifique os logs para mais detalhes sobre os erros");
        else
            return queryRelationShip;
    }

    private void verifyDuplicateId(Table table, String key, Session session) {
        String queryId =  getQueryAttribute(table.getName(), _idColumnName, key);
        List<Record> results = session.run(queryId).list();

        if(results.size() >= 1)
            throw new UnsupportedOperationException("Duplicate register for id " + key);
    }

    private Map<String, Object> getStringObjectMap(String key, LinkedList<String> cols, ArrayList<String> values) {
        Map<String,Object> props = new HashMap<>();
        boolean contaisId = false;
        for (int i = 0; i < cols.size(); i++)
        {
            String name = cols.get(i);
            String value = values.get(i);

            try
            {
                props.put( name, Integer.parseInt(value));
            }catch (NumberFormatException ex)
            {
                try
                {
                    props.put( name, Double.parseDouble(value));
                }catch (NumberFormatException ex1)
                {
                    props.put( name, value);
                }
            }
            if(_idColumnName.equalsIgnoreCase(name))
                contaisId = true;
        }

        if(!contaisId)
            props.put( _idColumnName, key);
        return props;
    }

    private String getQueryAttribute(String table, String atribute, String value)
    {
        return "Match (n:"+ table + ") Where n." + atribute + "=" + value + " return n";
    }

    /**
     * @param table
     * @param key
     */
    @Override
    public void delete(String table, String key)
    {
        try (Session session = driver.session(SessionConfig.forDatabase(_nomeBancoDados)))
        {
            String queryDelete = "MATCH (n:" + table + ") " +
                                 "WHERE n." + _idColumnName + "=" + key +
                                 "DETACH DELETE n " +
                                 "return n";

            Result result = session.run(queryDelete);
            SummaryCounters ss = result.consume().counters();
            int deletedNodes = ss.nodesDeleted() + ss.relationshipsDeleted();
            verifyQueryResult(deletedNodes, queryDelete);

        }

    }

    private void verifyQueryResult(int affectedNodes, String query)
    {
        if(affectedNodes == 0)
            throw new UnsupportedOperationException("Os dados informados nao alteraram os dados do banco verifique a query. \n" + query);
    }

    /**
     * @param n
     * @param t
     * @param key
     * @return
     */
    @Override
    public HashMap<String, String> get(int n, String t, String key) {

        try (Session session = driver.session(SessionConfig.forDatabase(_nomeBancoDados)))
        {

            String querySelect = getQueryAttribute(t, _idColumnName, key);
            List<Record> results = session.run(querySelect).list();

            HashMap<String, String> props = new HashMap<>();
            for (int i = 0; i < results.size(); i++)
            {
                Map<String, Object> maps = results.get(i)
                        .fields()
                        .get(0)
                        .value()
                        .asMap();

                for (Map.Entry<String, Object> stringObjectEntry : maps.entrySet())
                {
                    String keyMap = stringObjectEntry.getKey();
                    String valueMap = stringObjectEntry.getValue().toString();
                    props.put(keyMap, valueMap);
                }
            }

            return props;
        }
    }

    @Override
    public String toString() {
        return DbName;
    }

    @Override
    public void close() {
        driver.close();
    }
}
