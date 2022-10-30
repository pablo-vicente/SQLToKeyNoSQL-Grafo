package com.lisa.sqltokeynosql.util.connectors.neo4j;

import com.lisa.sqltokeynosql.architecture.Connector;
import com.lisa.sqltokeynosql.util.AlterDto;
import com.lisa.sqltokeynosql.util.report.TimeReportService;
import com.lisa.sqltokeynosql.util.sql.Table;
import org.json.JSONArray;
import org.neo4j.driver.*;
import org.neo4j.driver.Record;
import org.neo4j.driver.summary.SummaryCounters;

import java.util.*;

public class Neo4jConnector extends Connector
{
    private final Driver driver;
    private Session Session;
    private String _nomeBancoDados = "";

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
        var constraintName = Neo4jUtils.getContraintNodeKeyName(tableName);

        var sb = new StringBuilder();
        sb.append("CREATE CONSTRAINT ").append(constraintName).append("\n")
                .append("IF NOT EXISTS FOR (n:").append(tableName).append(")\n")
                .append("REQUIRE (n.").append(Neo4jUtils._nodeKey).append(") IS UNIQUE");

        var query = sb.toString();

        var stopwatchCreate = TimeReportService.CreateAndStartStopwatch();
        Result result = Session.run(query);
        TimeReportService.putTimeNeo4j(CREATE,stopwatchCreate);

        SummaryCounters summaryCounters = result.consume().counters();
        Neo4jUtils.verifyQueryResult(summaryCounters, query);
        TimeReportService.putTimeConnector(CREATE,stopwatchCreateConnetor);
    }

    @Override
    public void alter(Table table, ArrayList<AlterDto> dados)
    {

        String ALTER = "ALTER";
        var stopwatchAlterConnetor = TimeReportService.CreateAndStartStopwatch();

        var queries = new StringBuilder();
        var queriesRelationships= new ArrayList<StringBuilder>();
        var shortName = "n";
        queriesRelationships.add(new StringBuilder()
                .append("MATCH(")
                .append(shortName)
                .append(":")
                .append(table.getName())
                .append(")"));

        for (AlterDto dado : dados)
        {
            var shortNameRelacao = shortName + dado.ColunaExistente;
            if(dado.ChaveEstrangeira)
                queriesRelationships.add(new StringBuilder()
                        .append("OPTIONAL MATCH(")
                        .append(shortName)
                        .append(":")
                        .append(table.getName())
                        .append(") -[")
                        .append(dado.ColunaExistente)
                        .append(":")
                        .append(dado.ColunaExistente)
                        .append("]-> (")
                        .append(shortNameRelacao)
                        .append(")"));

            switch (dado.AlterOperation)
            {
                case DROP:
                    queries.append("REMOVE ").append(shortName).append(".").append(dado.ColunaExistente).append("\n");

                    if(dado.ChaveEstrangeira)
                        queries.append("DELETE ").append(dado.ColunaExistente).append("\n");

                    break;

                case RENAME:

                    queries.append("SET ").append(shortName).append(".").append(dado.ColunaNova).append(" = ").append(shortName).append(".").append(dado.ColunaExistente).append("\n");
                    queries.append("REMOVE ").append(shortName).append(".").append(dado.ColunaExistente).append("\n");
                    if(dado.ChaveEstrangeira)
                    {
                        queries.append("CREATE (").append(shortName).append(")-[:").append(dado.ColunaNova).append("]->(").append(shortNameRelacao).append(")").append("\n");
                        queries.append("DELETE ").append(dado.ColunaExistente).append("\n");
                    }

                    break;

                default:
                    throw new UnsupportedOperationException("Operação Não Suportada!!");
            }
        }

        var query = String.join("\n", queriesRelationships) +
                "\n"
                + queries;

        var stopwatchAlter = TimeReportService.CreateAndStartStopwatch();
        var result = Session.run(query);
        TimeReportService.putTimeNeo4j(ALTER, stopwatchAlter);

        SummaryCounters summaryCounters = result.consume().counters();
        Neo4jUtils.verifyQueryResult(summaryCounters, query);

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
        var constraintName = Neo4jUtils.getContraintNodeKeyName(tableName);
        var queryDropConstraints = "DROP CONSTRAINT " + constraintName + " IF EXISTS" ;

        var stopwatchDrop = TimeReportService.CreateAndStartStopwatch();
        var summaryCountersDropNodes = Session.run(queryDropNodes).consume().counters();
        var summaryCountersDropConstraints = Session.run(queryDropConstraints).consume().counters();
        TimeReportService.putTimeNeo4j(DROP, stopwatchDrop);

        var resuts  = new ArrayList<SummaryCounters>();
        resuts.add(summaryCountersDropNodes);
        resuts.add(summaryCountersDropConstraints);
        Neo4jUtils.verifyQueryResult(resuts, queryDropNodes + "\n" + queryDropConstraints);

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

        String separador01 = "\n   ";
        String separador02 = ",\n   ";
        for (var tuple : dados.entrySet())
        {
            var queryInsert = new StringBuilder();

            var key = tuple.getKey();
            var node = new StringBuilder().append(table.getName()).append(key);
            var values = tuple.getValue();

            var relationships = Neo4jUtils.queryRelationships(table, cols, values, node.toString());

            if(relationships.match.size() != 0)
            {
                var matchs = String.join(separador02, relationships.match);
                queryInsert.append("MATCH").append(separador01).append(matchs).append("\n");
            }

            var propsName = new StringBuilder().append("props").append(key);
            Map<String, Object> props = Neo4jUtils.getStringObjectMap(key, cols, values);
            Map<String,Object> params = new HashMap<>();
            params.put(propsName.toString(), props);

            queryInsert
                    .append("CREATE").append(separador01)
                    .append("(").append(node).append(":").append(table.getName()).append(" $").append(propsName).append(")");

            if(relationships.create.size() != 0)
            {
                queryInsert.append(separador02);

                var creates = String.join(separador02, relationships.create);
                queryInsert.append(creates).append("\n");
            }

            var stopwatchInsert = TimeReportService.CreateAndStartStopwatch();
            var result = Session.run(queryInsert.toString(), params);
            TimeReportService.putTimeNeo4j(PUT, stopwatchInsert);

            var relationsShips = result.consume().counters();

            if(relationsShips.relationshipsCreated() != relationships.columnsFks.size()
                    && relationsShips.nodesCreated() != 1)
                throw new UnsupportedOperationException("Não foi possível criar relacionamentos. Chaves estrangeiras do relacionamento não estao inseridas no banco!" + "\n" + queryInsert);
        }

        TimeReportService.putTimeConnector(PUT,stopwatchPutConnetor);

    }

    @Override
    public void update(Table table, HashMap<String, ArrayList<String>> dataSet, List<String> colunas, List<String> valores)
    {
        String UPDATE = "UPDATE";
        var stopwatchUPDATEConnetor = TimeReportService.CreateAndStartStopwatch();

        var node = "n";
        var relationships = Neo4jUtils.verifyRelationships(table, colunas, valores, node);
        var queriesVerificacaoDistinct = relationships.queriesVerifyFks;

        if(queriesVerificacaoDistinct.size() > 0)
        {
            var queryVerificaca = String.join("\nUNION\n", queriesVerificacaoDistinct);
            var stopwatchPut = TimeReportService.CreateAndStartStopwatch();
            var resultVerificacao = Session.run(queryVerificaca);
            TimeReportService.putTimeNeo4j(UPDATE, stopwatchPut);
            var relationsShips = resultVerificacao
                    .list()
                    .size();

            if(relationsShips != queriesVerificacaoDistinct.size())
            {
                System.out.println(resultVerificacao);
                throw new UnsupportedOperationException("Não foi possível criar relacionamentos. Chaves estrangeiras do relacionamento não estao inseridas no banco!" + "\n" + queryVerificaca);
            }
        }

        var propsName = "props";
        Map<String, Object> props = Neo4jUtils.getStringObjectMap(colunas, valores);
        Map<String,Object> params = new HashMap<>();
        params.put(propsName, props);

        StringBuilder queryUpdate = new StringBuilder();

        queryUpdate
                .append("MATCH (").append(node).append(":").append(table.getName()).append(")").append("\n")
                .append("WHERE ").append(node).append(".").append(Neo4jUtils._nodeKey).append(" in ").append(dataSet.keySet()).append("\n")
                .append("SET ").append(node).append(" += $").append(propsName).append("\n")
                .append("\n");

        if(queriesVerificacaoDistinct.size() != 0)
        {
            var queryRelationships = String.join("\n", relationships.querysRelationships);
            var colunasrelacionamento = new JSONArray(relationships.columnsFks);
            queryUpdate
                    .append("WITH ").append(node).append("\n")
                    .append("CALL") .append("\n")
                    .append("   {").append("\n")
                    .append("   WITH ").append(node).append("\n")
                    .append("   MATCH (").append(node).append(")-[relacionamento]->(relacionamento_apontado)").append("\n")
                    .append("   WHERE type(relacionamento) in ").append(colunasrelacionamento).append("\n")
                    .append("   DELETE relacionamento").append("\n")
                    .append("}").append("\n")
                    .append("\n")
                    .append(queryRelationships).append("\n");
        }

        var stopwatchInsert = TimeReportService.CreateAndStartStopwatch();
        Session.run(queryUpdate.toString(), params).consume();
        TimeReportService.putTimeNeo4j(UPDATE, stopwatchInsert);

        TimeReportService.putTimeConnector(UPDATE,stopwatchUPDATEConnetor);
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
        var queryDelete = Neo4jUtils.queryDelete(table, keys);

        var stopwatchDelete = TimeReportService.CreateAndStartStopwatch();
        Result result = Session.run(queryDelete);
        TimeReportService.putTimeNeo4j(DELETE, stopwatchDelete);

        SummaryCounters summaryCounters = result.consume().counters();
        Neo4jUtils.verifyQueryResult(summaryCounters, queryDelete);

        TimeReportService.putTimeConnector(DELETE,stopwatchDeleteConnetor);
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

        String querySelect = Neo4jUtils.getQueryAttribute(table, Neo4jUtils._nodeKey, key);

        var stopwatchGet = TimeReportService.CreateAndStartStopwatch();
        List<Record> results = Session.run(querySelect).list();
        TimeReportService.putTimeNeo4j(GET, stopwatchGet);

        HashMap<String, String> props = new HashMap<>();
        for (Record result : results) props = Neo4jUtils.getStringStringHashMap(result);

        TimeReportService.putTimeConnector(GET, stopwatchDeleteConnetor);
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
            var tuple = Neo4jUtils.getStringStringHashMap(record);
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
}
