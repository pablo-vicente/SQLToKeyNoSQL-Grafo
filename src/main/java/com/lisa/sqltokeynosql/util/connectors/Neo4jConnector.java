package com.lisa.sqltokeynosql.util.connectors;

import com.lisa.sqltokeynosql.architecture.Connector;
import com.lisa.sqltokeynosql.architecture.Parser;
import com.lisa.sqltokeynosql.util.TimeReport;
import com.lisa.sqltokeynosql.util.sql.ForeignKey;
import com.lisa.sqltokeynosql.util.sql.Table;
import org.neo4j.driver.*;
import org.neo4j.driver.Record;
import org.neo4j.driver.summary.SummaryCounters;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.neo4j.driver.Values.parameters;

public class Neo4jConnector extends Connector
{
    private final Driver driver;
    private Session Session;
    private String _nomeBancoDados = "";
    private final String _nodeKey = "NODE_KEY";

    public Neo4jConnector()
    {
        String uri = "bolt://localhost:7687";
        String user = "neo4j";
        String password = "pAsSw0rD";
        driver = GraphDatabase.driver( uri, AuthTokens.basic( user, password ));
    }


    /**
     * @param nameDB
     */
    @Override
    public void connect(String nameDB)
    {
        _nomeBancoDados = nameDB
                .trim()
                .toLowerCase()
                .replace("-", ".")
                .replace("_", ".");
        try (Session session = driver.session())
        {
            String query = "CREATE DATABASE " + _nomeBancoDados + " IF NOT EXISTS";
            session.run(query);
        }

        Session = driver.session(SessionConfig.forDatabase(_nomeBancoDados));
    }

    /**
     * @param table
     */
    @Override
    public void create(Table table)
    {
        var stopwatchCreateConnetor = new org.springframework.util.StopWatch();
        stopwatchCreateConnetor.start();
        var tableName = table.getName();
        var constraintName = getContraintNodeKeyName(tableName);

        var query = "CREATE CONSTRAINT " + constraintName +"\n"+
                "IF NOT EXISTS FOR (n:" + tableName + ")\n"+
                "REQUIRE (n." + _nodeKey + ") IS NODE KEY";

        var stopwatchCreate = new org.springframework.util.StopWatch();
        stopwatchCreate.start();
        Result result = Session.run(query);
        stopwatchCreate.stop();
        TimeReport.putTimeNeo4j("PUT",stopwatchCreate.getTotalTimeSeconds());

        SummaryCounters summaryCounters = result.consume().counters();
        verifyQueryResult(summaryCounters, query);
        stopwatchCreateConnetor.stop();
        TimeReport.putTimeConnector("PUT-CONNECTOR",stopwatchCreateConnetor.getTotalTimeSeconds());
    }

    private String getContraintNodeKeyName(String table)
    {
        return table + "_"+ "NODE_KEY";
    }

    @Override
    public void drop(Table table)
    {
        var stopwatchcDropConnetor = new org.springframework.util.StopWatch();
        stopwatchcDropConnetor.start();

        //TODO OTIMIZAR
        Session session1 = driver.session(SessionConfig.forDatabase(_nomeBancoDados));
        Session session2 = driver.session(SessionConfig.forDatabase(_nomeBancoDados));

        var tableName = table.getName();
        var queryDropNodes = "MATCH(n:" + tableName +") DETACH DELETE (n)";
        var constraintName = getContraintNodeKeyName(tableName);
        var queryDropConstraints = "DROP CONSTRAINT " + constraintName + " IF EXISTS" ;

        AtomicReference<Result> resultDropNodes = new AtomicReference<>();
        AtomicReference<Result> resultDropConstraints = new AtomicReference<>();

        Thread taskDropNodes = new Thread(() -> resultDropNodes.set(session1.run(queryDropNodes)));
        Thread taskDropConstraints = new Thread(() -> resultDropConstraints.set(session2.run(queryDropConstraints)));

        var stopwatchDropp = new org.springframework.util.StopWatch();
        stopwatchDropp.start();
        Stream.of(taskDropNodes, taskDropConstraints)
                .parallel()
                .forEach(r -> r.run());
        stopwatchDropp.stop();
        TimeReport.putTimeNeo4j("DROP",stopwatchDropp.getTotalTimeSeconds());

        SummaryCounters summaryCountersDropNodes = resultDropNodes.get().consume().counters();
        SummaryCounters summaryCountersDropConstraints = resultDropConstraints.get().consume().counters();

        var resuts  = new ArrayList<SummaryCounters>();
        resuts.add(summaryCountersDropNodes);
        resuts.add(summaryCountersDropConstraints);
        verifyQueryResult(resuts, queryDropNodes + "\n" + queryDropConstraints);

        stopwatchcDropConnetor.stop();
        TimeReport.putTimeConnector("DROP-CONNECTOR",stopwatchcDropConnetor.getTotalTimeSeconds());
    }

    /**
     * @param table
     * @param key
     * @param cols
     * @param values
     */
    @Override
    public void put(com.lisa.sqltokeynosql.util.Dictionary dictionary, Table table, String key, LinkedList<String> cols, ArrayList<String> values)
    {
        var stopwatchPutConnetor = new org.springframework.util.StopWatch();
        stopwatchPutConnetor.start();

        var node = table.getName() + key;

        var relationships = verifyRelationships(table, values, node).entrySet().iterator().next();
        var querysRelationships = relationships.getKey();
        var qntRelationships = relationships.getValue();

        var queryRelationShipWithtable = ReconstructionRelationshipWithtable(dictionary, table, node);

        Map<String, Object> props = getStringObjectMap(key, cols, values);
        String queryInsert =  "CREATE (" + node + ":" + table.getName() + " $props)\n" + querysRelationships + queryRelationShipWithtable;

        queryInsert += "WITH " + node + "\n"+
                    "MATCH ("+ node + ") --> (z) \n" +
                    "RETURN *";

        var stopwatchPut = new org.springframework.util.StopWatch();
        stopwatchPut.start();
        Result result = Session.run(queryInsert, parameters("props", props));
        stopwatchPut.stop();
        TimeReport.putTimeNeo4j("PUT",stopwatchPut.getTotalTimeSeconds());

        SummaryCounters summaryCounters = result.consume().counters();
        if(summaryCounters.relationshipsCreated() != qntRelationships || summaryCounters.nodesCreated() != 1)
        {
            Session.run("MATCH (n:"+ table.getName()+ ") WHERE n.NODE_KEY="+ props.get(_nodeKey) + " DETACH DELETE (n)");
            throw new UnsupportedOperationException("Não foi possível criar relacionamentos. Chaves estrangeiras do relacionamento não estao inseridas no banco!");
        }

        stopwatchPutConnetor.stop();
        TimeReport.putTimeConnector("PUT-CONNECTOR",stopwatchPutConnetor.getTotalTimeSeconds());
    }

    private String ReconstructionRelationshipWithtable(com.lisa.sqltokeynosql.util.Dictionary dictionary, Table table, String node)
    {
        var tables = dictionary.getCurrentDb().getTables();
        String queryRelationshipWithtable = "";

        for (Table tableWithFk : tables)
        {
            if(tableWithFk.getName().equalsIgnoreCase(table.getName()))
                continue;

            var fks = tableWithFk.getFks();
            for (ForeignKey foreignKey : fks)
            {
                if(!foreignKey.getrTable().equalsIgnoreCase(table.getName()))
                    continue;

                var referenceAttribute = foreignKey.getrAtt();
                var attribute = foreignKey.getAtt();

                var nodeShortCutName = tableWithFk.getName() + "_" + referenceAttribute + "_" + attribute;
                queryRelationshipWithtable +=  "WITH (" +  node +")\n" +
                        "MATCH (" + nodeShortCutName + ":" + tableWithFk.getName() + ")\n" +
                        "WHERE " + nodeShortCutName + "." + attribute + " = " + node +"." + referenceAttribute + "\n" +
                        "CREATE (" + nodeShortCutName + ")-[:" + referenceAttribute + "]->(" + node + ")\n";
            }
        }

        return queryRelationshipWithtable;
    }

    private HashMap<String, Integer> verifyRelationships(Table table, ArrayList<String> values, String node)
    {
        String queryRelationShip = "";
        Integer fksNecessarias = 0;
        var fks = table.getFks();
        for (ForeignKey fk : fks)
        {
            var attribute = fk.getAtt();
            int indexFkAttribute = table.getAttributes().indexOf(attribute);
            var valueFkAttribute = values.get(indexFkAttribute);

            if(valueFkAttribute.equalsIgnoreCase("NULL"))
                continue;

            fksNecessarias++;

            var referenceTable = fk.getrTable();
            var referenceAttribute = fk.getrAtt();

            var fkShortName = referenceTable + "_" + referenceAttribute + "_" + attribute;

            queryRelationShip += "WITH (" + node + ")\n" +
                    "MATCH (" + fkShortName + ":" + referenceTable + ")\n" +
                    "WHERE " + fkShortName + "." + referenceAttribute + " = " + valueFkAttribute + "\n" +
                    "CREATE (" + node + ")-[:" + referenceAttribute + "]->(" + fkShortName + ")\n";
        }

        var result = new HashMap<String, Integer>();
        result.put(queryRelationShip, fksNecessarias);

        return result;
    }

    private void verifyDuplicateId(Table table, String key, Session session)
    {
        String queryId =  getQueryAttribute(table.getName(), _nodeKey, key);
        List<Record> results = session.run(queryId).list();

        if(results.size() >= 1)
            throw new UnsupportedOperationException("Duplicate register for id " + key);
    }

    private Map<String, Object> getStringObjectMap(String key, LinkedList<String> cols, ArrayList<String> values)
    {
        Map<String,Object> props = new HashMap<>();
        for (int i = 0; i < cols.size(); i++)
        {
            String name = Parser.removeInvalidCaracteres(cols.get(i));
            String value = Parser.removeInvalidCaracteres(values.get(i));

            try
            {
                props.put( name, Integer.parseInt(value));
            }
            catch (NumberFormatException ex)
            {
                try
                {
                    props.put( name, Double.parseDouble(value));
                }catch (NumberFormatException ex1)
                {
                    props.put( name, value);
                }
            }
        }

        props.put(_nodeKey, Integer.parseInt(key));

        return props;
    }

    private String getQueryAttribute(String table, String atribute, String value)
    {
        return "MATCH (n:"+ table + ") " +
                "WHERE n." + atribute + "=" + value + " " +
                "RETURN n";
    }

    /**
     * @param table
     * @param key
     */
    @Override
    public void delete(String table, String key)
    {
        var stopwatchDeleteConnetor = new org.springframework.util.StopWatch();
        stopwatchDeleteConnetor.start();
        String queryDelete = "MATCH (n:" + table + ") " +
                "WHERE n." + _nodeKey + "=" + key + " " +
                "DETACH DELETE n";

        var stopwatchDelete = new org.springframework.util.StopWatch();
        stopwatchDelete.start();
        Result result = Session.run(queryDelete);
        stopwatchDelete.stop();
        TimeReport.putTimeNeo4j("DELETE",stopwatchDelete.getTotalTimeSeconds());

        SummaryCounters summaryCounters = result.consume().counters();
        verifyQueryResult(summaryCounters, queryDelete);

        stopwatchDeleteConnetor.stop();
        TimeReport.putTimeConnector("DELETE-CONNECTOR",stopwatchDeleteConnetor.getTotalTimeSeconds());
    }

    private void verifyQueryResult(SummaryCounters summaryCounters, String query)
    {
        var summarysCounters = new ArrayList<SummaryCounters>();
        summarysCounters.add(summaryCounters);
        verifyQueryResult(summarysCounters, query);
    }
    private void verifyQueryResult(ArrayList<SummaryCounters> summaryCounters, String query)
    {
        var affectedNodes = 0;

        for (SummaryCounters summaryCounter : summaryCounters)
        {
            affectedNodes += summaryCounter.nodesCreated();
            affectedNodes += summaryCounter.nodesDeleted();
            affectedNodes += summaryCounter.relationshipsCreated();
            affectedNodes += summaryCounter.relationshipsDeleted();
            affectedNodes += summaryCounter.propertiesSet();
            affectedNodes += summaryCounter.labelsAdded();
            affectedNodes += summaryCounter.labelsRemoved();
            affectedNodes += summaryCounter.indexesAdded();
            affectedNodes += summaryCounter.indexesRemoved();
            affectedNodes += summaryCounter.constraintsAdded();
            affectedNodes += summaryCounter.constraintsRemoved();
            affectedNodes += summaryCounter.systemUpdates();
        }

        if(affectedNodes == 0)
            throw new UnsupportedOperationException("Os dados informados nao alteraram os dados do banco verifique a query. \n" + query);
    }

    /**
     * @param n
     * @param table
     * @param key
     * @return
     */
    @Override
    public HashMap<String, String> get(int n, String table, String key)
    {
        var stopwatchDeleteConnetor = new org.springframework.util.StopWatch();
        stopwatchDeleteConnetor.start();
        String querySelect = getQueryAttribute(table, _nodeKey, key);

        var stopwatchGet = new org.springframework.util.StopWatch();
        stopwatchGet.start();
        List<Record> results = Session.run(querySelect).list();
        stopwatchGet.stop();
        TimeReport.putTimeNeo4j("GET",stopwatchGet.getTotalTimeSeconds());

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

        stopwatchDeleteConnetor.stop();
        TimeReport.putTimeConnector("GET-CONNECTOR",stopwatchDeleteConnetor.getTotalTimeSeconds());
        return props;
    }
}
