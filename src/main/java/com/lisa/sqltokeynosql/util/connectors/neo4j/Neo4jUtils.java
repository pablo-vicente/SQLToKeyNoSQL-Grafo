package com.lisa.sqltokeynosql.util.connectors.neo4j;

import com.lisa.sqltokeynosql.architecture.Parser;
import com.lisa.sqltokeynosql.util.sql.ForeignKey;
import com.lisa.sqltokeynosql.util.sql.Table;
import org.neo4j.driver.Record;
import org.neo4j.driver.summary.SummaryCounters;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Neo4jUtils
{
    public static final String _nodeKey = "NODE_KEY";
    public static String getContraintNodeKeyName(String table)
    {
        return new StringBuilder().append(table).append("_").append("NODE_KEY").toString();
    }

    public static RelationshipDto queryRelationships(Table table, List<String> colunas, List<String> values, String node)
    {
        var sbsMatch = new ArrayList<StringBuilder>();
        var sbsCreate = new ArrayList<StringBuilder>();
        var columnsFks = new ArrayList<String>();

        var fks = table.getFks();
        for (ForeignKey fk : fks)
        {
            var attribute = fk.getAtt();
            int indexFkAttribute = colunas.indexOf(attribute);

            if(indexFkAttribute == -1)
                continue;
            columnsFks.add(attribute);
            var valueFkAttribute = values.get(indexFkAttribute);

            if(valueFkAttribute.equalsIgnoreCase("NULL"))
                continue;

            var referenceTable = fk.getrTable();
            var referenceAttribute = fk.getrAtt();

            var fkShortName = new StringBuilder()
                    .append(referenceTable)
                    .append("_")
                    .append(referenceAttribute)
                    .append("_")
                    .append(attribute)
                    .append(valueFkAttribute);

            var sbMach = new StringBuilder();
            sbMach
                    .append("(")
                    .append(fkShortName) // (tabelaEstrangeira_atributoEstrangeira_atributo
                    .append(":").append(referenceTable)
                    .append("{").append(_nodeKey).append(":").append(valueFkAttribute).append("}") // {NODE_KEY:1}
                    .append(")");
            sbsMatch.add(sbMach);

            var sbCreate = new StringBuilder();
            sbCreate
                    .append("(").append(node).append(")-[")
                    .append(attribute).append(":").append(attribute)
                    .append("]->(").append(fkShortName).append(")");// (funcionario1)-[funcao_id:funcao_id]->(funcao_id_funcao_funcao_id)
            sbsCreate.add(sbCreate);
        }

        return new RelationshipDto(sbsMatch, sbsCreate, columnsFks);
    }

    public static VerifyRelationshipDto verifyRelationships(Table table, List<String> colunas, List<String> values, String node)
    {
        var queryRelationShip = new ArrayList<StringBuilder>();

        var columnsFks = new ArrayList<String>();
        var fks = table.getFks();
        var queryVerifyFks = new ArrayList<StringBuilder>();
        for (ForeignKey fk : fks)
        {
            var attribute = fk.getAtt();
            int indexFkAttribute = colunas.indexOf(attribute);

            if(indexFkAttribute == -1)
                continue;
            columnsFks.add(attribute);
            var valueFkAttribute = values.get(indexFkAttribute);

            if(valueFkAttribute.equalsIgnoreCase("NULL"))
                continue;

            var referenceTable = fk.getrTable();
            var referenceAttribute = fk.getrAtt();

            var fkShortName = new StringBuilder()
                    .append(referenceTable)
                    .append("_")
                    .append(referenceAttribute)
                    .append("_")
                    .append(attribute);

            var sbqueryRelationShip = new StringBuilder();
            sbqueryRelationShip
                    .append("WITH (").append(node).append(")\n")
                    .append("MATCH (").append(fkShortName).append(":").append(referenceTable).append(")\n")
                    .append("WHERE ").append(fkShortName).append(".").append(referenceAttribute).append(" = ").append(valueFkAttribute).append("\n")
                    .append("CREATE (").append(node).append(")-[:").append(attribute).append("]->(").append(fkShortName).append(")\n");
            queryRelationShip.add(sbqueryRelationShip);

            var sbqueryVerifyFks = new StringBuilder();
            sbqueryVerifyFks.append("MATCH (n:").append(referenceTable).append(")\n")
                    .append("WHERE n.").append(referenceAttribute).append(" = ").append(valueFkAttribute).append("\n")
                    .append("RETURN (n)\n");
            queryVerifyFks.add(sbqueryVerifyFks);
        }

        return new VerifyRelationshipDto(queryRelationShip, queryVerifyFks, columnsFks);
    }

    public static Map<String, Object> getStringObjectMap(String key, List<String> cols, List<String> values)
    {
        Map<String, Object> props = getStringObjectMap(cols, values);
        props.put(_nodeKey, Integer.parseInt(key));
        return props;
    }

    public static Map<String, Object> getStringObjectMap(List<String> cols, List<String> values) {
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
        return props;
    }

    public static String getQueryAttribute(String table, String atribute, String value)
    {
        return "MATCH (n:"+ table + ") " +
                "WHERE n." + atribute + "=" + value + " " +
                "RETURN n";
    }

    public static String queryDelete(String table, String...keys)
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

    public static void verifyQueryResult(SummaryCounters summaryCounters, String query)
    {
        var summarysCounters = new ArrayList<SummaryCounters>();
        summarysCounters.add(summaryCounters);
        verifyQueryResult(summarysCounters, query);
    }

    public static void verifyQueryResult(ArrayList<SummaryCounters> summaryCounters, String query)
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

    public static HashMap<String, String> getStringStringHashMap(Record record)
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
}
