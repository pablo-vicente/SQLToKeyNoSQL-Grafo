package com.lisa.sqltokeynosql.util.connectors;

import com.lisa.sqltokeynosql.architecture.Connector;
import com.lisa.sqltokeynosql.architecture.Parser;
import com.lisa.sqltokeynosql.util.AlterDto;
import com.lisa.sqltokeynosql.util.report.TimeReportService;
import com.lisa.sqltokeynosql.util.sql.ForeignKey;
import com.lisa.sqltokeynosql.util.sql.Table;
import org.neo4j.driver.*;
import org.neo4j.driver.Record;
import org.neo4j.driver.summary.SummaryCounters;
import org.springframework.util.StopWatch;

import java.util.*;
import java.util.stream.Collectors;


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
            session.run(query).consume();
        }

        Session = driver.session(SessionConfig.forDatabase(_nomeBancoDados));
    }

    /**
     * @param table
     */
    @Override
    public void create(Table table)
    {
        String CREATE = "CREATE";
        var stopwatchCreateConnetor = TimeReportService.CreateAndStartStopwatch();

        var tableName = table.getName();
        var constraintName = getContraintNodeKeyName(tableName);

        var query = "CREATE CONSTRAINT " + constraintName +"\n"+
                "IF NOT EXISTS FOR (n:" + tableName + ")\n"+
                "REQUIRE (n." + _nodeKey + ") IS UNIQUE";
//                "REQUIRE (n." + _nodeKey + ") IS NODE KEY";

        var stopwatchCreate = TimeReportService.CreateAndStartStopwatch();
        Result result = Session.run(query);
        TimeReportService.putTimeNeo4j(CREATE,stopwatchCreate);

        SummaryCounters summaryCounters = result.consume().counters();
        verifyQueryResult(summaryCounters, query);
        TimeReportService.putTimeConnector(CREATE,stopwatchCreateConnetor);
    }

    private String getContraintNodeKeyName(String table)
    {
        return table + "_"+ "NODE_KEY";
    }

    @Override
    public void alter(Table table, ArrayList<AlterDto> dados)
    {

        String ALTER = "ALTER";
        var stopwatchAlterConnetor = TimeReportService.CreateAndStartStopwatch();

        var queries = new ArrayList<String>();
        var queriesRelationships= new ArrayList<String>();
        var shortName = "n";
        queriesRelationships.add("MATCH(" + shortName + ":" + table.getName() + ")");

        for (AlterDto dado : dados)
        {
            var shortNameRelacao = shortName + dado.ColunaExistente;
            if(dado.ChaveEstrangeira)
                queriesRelationships.add("OPTIONAL MATCH(" + shortName + ":" + table.getName() + ") -[" + dado.ColunaExistente + ":" + dado.ColunaExistente + "]-> (" + shortNameRelacao + ")");

            switch (dado.AlterOperation)
            {
                case DROP:
                    queries.add("REMOVE " + shortName + "." + dado.ColunaExistente);

                    if(dado.ChaveEstrangeira)
                        queries.add("DELETE " + dado.ColunaExistente);

                    break;

                case RENAME:

                    queries.add("SET " + shortName + "." + dado.ColunaNova + " = " + shortName + "." + dado.ColunaExistente);
                    queries.add("REMOVE " + shortName + "." + dado.ColunaExistente);
                    if(dado.ChaveEstrangeira)
                    {
                        queries.add("CREATE (" + shortName +  ")-[:" + dado.ColunaNova +"]->(" + shortNameRelacao + ")");
                        queries.add("DELETE " + dado.ColunaExistente);
                    }

                    break;

                default:
                    throw new UnsupportedOperationException("Operação Não Suportada!!");
            }
        }

        var query = String.join("\n", queriesRelationships) +
                "\n"
                + String.join("\n", queries);

        var stopwatchAlter = TimeReportService.CreateAndStartStopwatch();
        var result = Session.run(query);
        TimeReportService.putTimeNeo4j(ALTER, stopwatchAlter);

        SummaryCounters summaryCounters = result.consume().counters();
        verifyQueryResult(summaryCounters, query);

        TimeReportService.putTimeConnector(ALTER, stopwatchAlterConnetor);
    }

    @Override
    public void drop(Table table)
    {
        String DROP = "DROP";
        var stopwatchcDropConnetor = TimeReportService.CreateAndStartStopwatch();

        var tableName = table.getName();
        var queryDropNodes = "MATCH(n:" + table.getName() + ")\n"+
                             "OPTIONAL MATCH(n:" + table.getName() + ") -[chave_estrangeira]-> (ce)\n" +
                             "DELETE n, chave_estrangeira";
        var constraintName = getContraintNodeKeyName(tableName);
        var queryDropConstraints = "DROP CONSTRAINT " + constraintName + " IF EXISTS" ;

        var stopwatchDrop = TimeReportService.CreateAndStartStopwatch();
        var summaryCountersDropNodes = Session.run(queryDropNodes).consume().counters();
        var summaryCountersDropConstraints = Session.run(queryDropConstraints).consume().counters();
        TimeReportService.putTimeNeo4j(DROP, stopwatchDrop);

        var resuts  = new ArrayList<SummaryCounters>();
        resuts.add(summaryCountersDropNodes);
        resuts.add(summaryCountersDropConstraints);
        verifyQueryResult(resuts, queryDropNodes + "\n" + queryDropConstraints);

        TimeReportService.putTimeConnector(DROP,stopwatchcDropConnetor);
    }

    /**
     * @param table
     * @param cols
     * @param dados
     */
    @Override
    public void put(Table table, List<String> cols, Map<String, List<String>> dados)
    {
        String PUT = "PUT";
        var stopwatchPutConnetor = TimeReportService.CreateAndStartStopwatch();

        var queriesInserts = new ArrayList<StringBuilder>();
        var queriesVerifyFks = new ArrayList<String>();

        Map<String,Object> params = new HashMap<>();
        for (var tuple : dados.entrySet())
        {
            StringBuilder queryInsert = new StringBuilder();

            var key = tuple.getKey();
            var node = table.getName() + key;
            var values = tuple.getValue();

            var relationships = verifyRelationships(table, values, node)
                    .entrySet()
                    .iterator()
                    .next();

            var querysRelationships = relationships.getKey();
            queriesVerifyFks.addAll(relationships.getValue());
            var queryRelationships = String.join("\n", querysRelationships);

            var propsName = "props" + key;
            Map<String, Object> props = getStringObjectMap(key, cols, values);
            params.put(propsName, props);

            queryInsert
                    .append("CREATE (").append(node).append(":").append(table.getName()).append(" $").append(propsName).append(")").append("\n")
                    .append(queryRelationships).append("\n");
//                    .append("WITH ").append(node).append("\n")
//                    .append("MATCH (").append(node).append(") -[chave_estrangeira]-> (apontado)").append("\n")
//                    .append("RETURN (chave_estrangeira)").append("\n");

            queriesInserts.add(queryInsert);

        }

        InserirDados(PUT, stopwatchPutConnetor, queriesInserts, queriesVerifyFks, params);
    }

    private void InserirDados(
            String PUT,
            StopWatch stopwatchPutConnetor,
            List<StringBuilder> queriesInserts,
            List<String> queriesVerifyFks,
            Map<String, Object> params)
    {
        var queriesVerificacaoDistinct = queriesVerifyFks
                .stream()
                .distinct()
                .collect(Collectors.toList());

        if(queriesVerificacaoDistinct.size() > 0)
        {
            var queryVerificaca = String.join("\nUNION\n", queriesVerificacaoDistinct);
            var stopwatchPut = TimeReportService.CreateAndStartStopwatch();
            var resultVerificacao = Session.run(queryVerificaca);
            TimeReportService.putTimeNeo4j(PUT, stopwatchPut);
            var relationsShips = resultVerificacao
                    .list()
                    .size();
            if(relationsShips != queriesVerificacaoDistinct.size())
            {
                System.out.println(resultVerificacao);
                throw new UnsupportedOperationException("Não foi possível criar relacionamentos. Chaves estrangeiras do relacionamento não estao inseridas no banco!" + "\n" + queryVerificaca);
            }
        }

        var query = String.join("\nUNION\n\n", queriesInserts);
        var stopwatchInsert = TimeReportService.CreateAndStartStopwatch();
        Session.run(query, params).consume();
        TimeReportService.putTimeNeo4j(PUT, stopwatchInsert);

        TimeReportService.putTimeConnector(PUT,stopwatchPutConnetor);
    }

    private HashMap<ArrayList<String>, ArrayList<String>> verifyRelationships(Table table, List<String> values, String node)
    {
        var queryRelationShip = new ArrayList<String>();

        var fks = table.getFks();
        var queryVerifyFks = new ArrayList<String>();
        for (ForeignKey fk : fks)
        {
            var attribute = fk.getAtt();
            int indexFkAttribute = table.getAttributes().indexOf(attribute);
            var valueFkAttribute = values.get(indexFkAttribute);

            if(valueFkAttribute.equalsIgnoreCase("NULL"))
                continue;

            var referenceTable = fk.getrTable();
            var referenceAttribute = fk.getrAtt();

            var fkShortName = referenceTable + "_" + referenceAttribute + "_" + attribute;

            queryRelationShip.add("WITH (" + node + ")\n" +
                    "MATCH (" + fkShortName + ":" + referenceTable + ")\n" +
                    "WHERE " + fkShortName + "." + referenceAttribute + " = " + valueFkAttribute + "\n" +
                    "CREATE (" + node + ")-[:" + attribute + "]->(" + fkShortName + ")\n");

            queryVerifyFks.add(
                    "MATCH (n:" + referenceTable + ")\n" +
                    "WHERE n." + referenceAttribute + " = " + valueFkAttribute + "\n" +
                    "RETURN (n)\n");
        }

        var result = new HashMap<ArrayList<String>, ArrayList<String>>();
        result.put(queryRelationShip,  queryVerifyFks);

        return result;
    }

    private Map<String, Object> getStringObjectMap(String key, List<String> cols, List<String> values)
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

    private String QueryDelete(String table, String...keys)
    {
        StringBuilder query = new StringBuilder();
        for (String key : keys)
        {
            query
                    .append("MATCH(n:")
                    .append(table)
                    .append(")\n")
                    .append("OPTIONAL MATCH(n:")
                    .append(table)
                    .append(") -[chaves_estrangeiras]-> (ce)\n")
                    .append("WHERE n.")
                    .append(_nodeKey)
                    .append(" in ")
                    .append(key)
                    .append("\n")
                    .append("DELETE chaves_estrangeiras,n");
        }

        return query.toString();
    }

    /**
     * @param table
     * @param keys
     */
    @Override
    public void delete(String table, String...keys)
    {
        String DELETE = "DELETE";
        var stopwatchDeleteConnetor = TimeReportService.CreateAndStartStopwatch();
        var queryDelete = QueryDelete(table, keys);

        var stopwatchDelete = TimeReportService.CreateAndStartStopwatch();
        Result result = Session.run(queryDelete);
        TimeReportService.putTimeNeo4j(DELETE, stopwatchDelete);

        SummaryCounters summaryCounters = result.consume().counters();
        verifyQueryResult(summaryCounters, queryDelete);

        TimeReportService.putTimeConnector(DELETE,stopwatchDeleteConnetor);
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
        String GET = "GET";
        var stopwatchDeleteConnetor = TimeReportService.CreateAndStartStopwatch();

        String querySelect = getQueryAttribute(table, _nodeKey, key);

        var stopwatchGet = TimeReportService.CreateAndStartStopwatch();
        List<Record> results = Session.run(querySelect).list();
        TimeReportService.putTimeNeo4j(GET, stopwatchGet);

        HashMap<String, String> props = new HashMap<>();
        for (Record result : results) props = getStringStringHashMap(result);

        TimeReportService.putTimeConnector(GET, stopwatchDeleteConnetor);
        return props;
    }

    private static HashMap<String, String> getStringStringHashMap(Record record)
    {
        HashMap<String, String> props = new HashMap<>();
        var tuple = record
                .get(0)
                .asMap(x -> Parser.removeInvalidCaracteres(x.toString()));

        for (var stringObjectEntry : tuple.entrySet())
        {
            String keyMap = stringObjectEntry.getKey();
            String valueMap = stringObjectEntry.getValue();
            props.put(keyMap, valueMap);
        }
        return props;
    }

    @Override
    public ArrayList getN(int n, String table, ArrayList<String> keys, Stack<Object> filters, LinkedList<String> cols)
    {
        String CREATE = "GETN";
        var stopwatchGetNConnetor = TimeReportService.CreateAndStartStopwatch();
        var query = "MATCH(n:" + table + ") RETURN (n)";

        var stopwatchGetN = TimeReportService.CreateAndStartStopwatch();
        var result = Session.run(query).list();
        TimeReportService.putTimeNeo4j(CREATE, stopwatchGetN);

        var results = new ArrayList<String[]>();
        for (Record record : result)
        {
            var tuple = getStringStringHashMap(record);
            if(!applyFilterR(filters, tuple))
                continue;

            var tupleR = new String[cols.size()];
            for (int i = 0; i < cols.size(); i++)
            {
                var col = cols.get(i);
                var data = tuple.get(col);
                tupleR[i] = data;
            }

            results.add(tupleR);
        }

        TimeReportService.putTimeConnector(CREATE,stopwatchGetNConnetor);

        return results;
    }

    @Override
    public void update(Table table, HashMap<String, ArrayList<String>> dataSet)
    {
        String UPDATE = "UPDATE";
        var stopwatchUPDATEConnetor = TimeReportService.CreateAndStartStopwatch();

        var queriesUpdates = new ArrayList<StringBuilder>();
        var queriesVerifyFks = new ArrayList<String>();

        Map<String,Object> params = new HashMap<>();
        var cols = table.getAttributes();
        for (var tuple : dataSet.entrySet())
        {
            StringBuilder queryUpdate = new StringBuilder();

            var key = tuple.getKey();
            var node = table.getName() + key;
            var values = tuple.getValue();

            var relationships = verifyRelationships(table, values, node)
                    .entrySet()
                    .iterator()
                    .next();

            var querysRelationships = relationships.getKey();
            queriesVerifyFks.addAll(relationships.getValue());
            var queryRelationships = String.join("\n", querysRelationships);

            var propsName = "props" + key;
            Map<String, Object> props = getStringObjectMap(key, cols, values);
            params.put(propsName, props);

            queryUpdate
                    .append("MATCH (").append(node).append(":").append(table.getName()).append(")").append("\n")
                    .append("WHERE ").append(node).append(".").append(_nodeKey).append("=").append(key).append("\n")
                    .append("SET ").append(node).append(" = $").append(propsName).append("\n")
                    .append("\n")
                    .append("WITH ").append(node).append("\n")
                    .append("CALL") .append("\n")
                    .append("   {").append("\n")
                    .append("   WITH ").append(node).append("\n")
                    .append("   MATCH (").append(node).append(")-[relacionamento]->(relacionamento_apontado) DELETE relacionamento").append("\n")
                    .append("}").append("\n")
                    .append("\n")
                    .append(queryRelationships).append("\n");
//                    .append("WITH ").append(node).append("\n")
//                    .append("MATCH (").append(node).append(") -[chave_estrangeira]-> (apontado)").append("\n")
//                    .append("RETURN (chave_estrangeira)").append("\n");

            queriesUpdates.add(queryUpdate);

        }

        InserirDados(UPDATE, stopwatchUPDATEConnetor, queriesUpdates, queriesVerifyFks, params);
    }
}
