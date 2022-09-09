package com.lisa.sqltokeynosql.util.connectors;

import com.lisa.sqltokeynosql.architecture.Connector;
import com.lisa.sqltokeynosql.architecture.Parser;
import com.lisa.sqltokeynosql.util.sql.ForeignKey;
import com.lisa.sqltokeynosql.util.sql.Table;
import org.neo4j.driver.*;
import org.neo4j.driver.Record;
import org.neo4j.driver.summary.SummaryCounters;

import java.util.*;
import java.util.stream.Collectors;

import static org.neo4j.driver.Values.parameters;

public class Neo4jConnector extends Connector
{
    private final Driver driver;
    private String _nomeBancoDados = "";
    private final String _idColumnName = "id";

    public Neo4jConnector()
    {
        String uri = "bolt://localhost:7687";
        String user = "neo4j";
        String password = "pAsSw0rD";
        driver = GraphDatabase.driver( uri, AuthTokens.basic( user, password ));
    }

    public Neo4jConnector(String uri, String user, String password)
    {
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
    }

    /**
     * @param table
     */
    @Override
    public void create(Table table)
    {
        try (Session session = driver.session(SessionConfig.forDatabase(_nomeBancoDados)))
        {
            var tableName = table.getName();
            var constraintName = tableName + "_"+ "NODE_KEY";

            var shortName = "n";

            var propertys = table
                    .getPks()
                    .stream()
                    .map(x -> shortName + "." + x)
                    .distinct()
                    .collect(Collectors.toList());
            var pk = shortName + "." + _idColumnName;
            if(!propertys.contains(pk))
                propertys.add(pk);

            var queryPropertys = String.join(",", propertys);
            var query = "CREATE CONSTRAINT " + constraintName +"\n"+
                    "IF NOT EXISTS FOR (n:" + tableName + ")\n"+
                    "REQUIRE (" + queryPropertys +  ") IS NODE KEY";

            Result result =session.run(query);
            SummaryCounters summaryCounters = result.consume().counters();
            verifyQueryResult(summaryCounters, query);
        }
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
        try (Session session = driver.session(SessionConfig.forDatabase(_nomeBancoDados)))
        {
            var node = table.getName() + key;
//            verifyDuplicateId(table, key, session);
            String querysRelationships = verifyRelationships(table, values, session, node);
            var queryRelationShipWithtable = ReconstructionRelationshipWithtable(dictionary, table, node);

            Map<String, Object> props = getStringObjectMap(key, cols, values);
            String queryInsert =  "CREATE (" + node + ":" + table.getName() + " $props)\n" +
                    querysRelationships + queryRelationShipWithtable;

            Result result =session.run(queryInsert, parameters("props", props));
            SummaryCounters summaryCounters = result.consume().counters();
            verifyQueryResult(summaryCounters, queryInsert);
        }
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

    private String verifyRelationships(Table table, ArrayList<String> values, Session session, String node)
    {
        String queryRelationShip = "";
        ArrayList<String> erros = new ArrayList<>();
        var pks = table.getFks();
        for (ForeignKey pk : pks)
        {
            var attribute = pk.getAtt();
            int indexFkAttribute = table.getAttributes().indexOf(attribute);
            var valueFkAttribute = values.get(indexFkAttribute);

            if(valueFkAttribute.equalsIgnoreCase("NULL"))
                continue;

            var referenceTable = pk.getrTable();
            var referenceAttribute = pk.getrAtt();

            var queryFk = getQueryAttribute(referenceTable, referenceAttribute, valueFkAttribute);
            List<Record> results = session.run(queryFk).list();

            if(results.size() != 1)
                erros.add("Nao foi possivel criar relacionamento entre atributo '" + attribute + " '" +
                        " com a tabela '" + referenceTable + " (" + referenceAttribute + ")'" +
                        " para o valor '" + valueFkAttribute + "' inexistente.");
            else
            {
                String nodeShortCutName = referenceTable + "_" + referenceAttribute + "_" + attribute;
                queryRelationShip +=    "WITH (" + node + ")\n" +
                        "MATCH (" + nodeShortCutName + ":" + referenceTable + ")\n" +
                        "WHERE " + nodeShortCutName + "." + referenceAttribute + " = " + valueFkAttribute + "\n" +
                        "CREATE (" + node + ")-[:" + referenceAttribute + "]->(" + nodeShortCutName + ")\n";
            }
        }

        for (String erro : erros)
            System.out.println(erro);

        if(erros.size() > 0)
            throw new UnsupportedOperationException("Não foi possível criar relacionamentos. Verifique os logs para mais detalhes sobre os erros");
        else
            return queryRelationShip;
    }

    private void verifyDuplicateId(Table table, String key, Session session)
    {
        String queryId =  getQueryAttribute(table.getName(), _idColumnName, key);
        List<Record> results = session.run(queryId).list();

        if(results.size() >= 1)
            throw new UnsupportedOperationException("Duplicate register for id " + key);
    }

    private Map<String, Object> getStringObjectMap(String key, LinkedList<String> cols, ArrayList<String> values)
    {
        Map<String,Object> props = new HashMap<>();
        boolean contaisId = false;
        for (int i = 0; i < cols.size(); i++)
        {
            String name = Parser.removeInvalidCaracteres(cols.get(i));
            String value = Parser.removeInvalidCaracteres(values.get(i));

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
            props.put( _idColumnName, Integer.parseInt(key));

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
        try (Session session = driver.session(SessionConfig.forDatabase(_nomeBancoDados)))
        {
            String queryDelete = "MATCH (n:" + table + ") " +
                    "WHERE n." + _idColumnName + "=" + key + " " +
                    "DETACH DELETE n";

            Result result = session.run(queryDelete);
            SummaryCounters summaryCounters = result.consume().counters();
            verifyQueryResult(summaryCounters, queryDelete);

        }
    }

    private void verifyQueryResult(SummaryCounters summaryCounters, String query)
    {
        var affectedNodes = 0;

        affectedNodes += summaryCounters.nodesCreated();
        affectedNodes += summaryCounters.nodesDeleted();
        affectedNodes += summaryCounters.relationshipsCreated();
        affectedNodes += summaryCounters.relationshipsDeleted();
        affectedNodes += summaryCounters.propertiesSet();
        affectedNodes += summaryCounters.labelsAdded();
        affectedNodes += summaryCounters.labelsRemoved();
        affectedNodes += summaryCounters.indexesAdded();
        affectedNodes += summaryCounters.indexesRemoved();
        affectedNodes += summaryCounters.constraintsAdded();
        affectedNodes += summaryCounters.constraintsRemoved();
        affectedNodes += summaryCounters.systemUpdates();

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
        try (Session session = driver.session(SessionConfig.forDatabase(_nomeBancoDados)))
        {

            String querySelect = getQueryAttribute(table, _idColumnName, key);
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
}
