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
            session.run("CREATE DATABASE $databasename IF NOT EXISTS",
                    parameters("databasename", _nomeBancoDados));
        }
        catch (Exception exception)
        {
            System.out.println(exception);
            throw exception;
        }
    }

    /**
     * @param table
     * @param key
     * @param cols
     * @param values
     */
    @Override
    public void put(Table table, String key, LinkedList<String> cols, ArrayList<String> values)
    {
        // TODO IMPLEMENTAT SEM PK
        // TODO IMPLEMENTAR COM PK

        try (Session session = driver.session(SessionConfig.forDatabase(_nomeBancoDados)))
        {
            verifyDuplicateId(table, key, session);
            verifyRelationships(table, values, session);

            Map<String, Object> props = getStringObjectMap(key, cols, values);
            String queryInsert = "CREATE (n:" + table.getName() + " $props)";

            session.run(queryInsert, parameters("props", props));
        }
        catch (Exception exception)
        {
            System.out.println(exception);
            throw exception;
        }

    }
    private void verifyRelationships(Table table, ArrayList<String> values, Session session)
    {
        ArrayList<String> erros = new ArrayList<>();
        ArrayList<ForeignKey> pks = table.getFks();
        for (int i = 0; i < pks.size(); i++) {
            ForeignKey pk = pks.get(i);

            String tableFk = pk.getrTable();
            String tableAttributeReference = pk.getrAtt();
            String tableAttribute = pk.getAtt();

            int indexPkReference = table.getAttributes().indexOf(tableAttribute);
            String fkReferenceValue = values.get(indexPkReference);
            String queryFk = getQueryAttribute(tableFk, tableAttributeReference, fkReferenceValue);
            List<Record> results = session.run(queryFk).list();

            if(results.size() != 1)
                erros.add("Não foi possivel criar relacionamento entre atributo " + tableAttribute +
                        " com a table " + tableFk + "(" + tableAttributeReference + ")" +
                        " para o valor " + fkReferenceValue);
        }

        for (int i = 0; i < erros.size(); i++) {
            System.out.println(erros.get(i));
        }

        if(erros.size() > 0)
            throw new UnsupportedOperationException("Não foi possível criar relacionamentos. Verifique os logs para mais detalhes sobre os erros");
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

            props.put( name, value );
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
    public void delete(String table, String key) {

    }

    /**
     * @param n
     * @param t
     * @param key
     * @return
     */
    @Override
    public HashMap<String, String> get(int n, String t, String key) {

//        "MATCH (n:Movie) RETURN n LIMIT 25"

        return null;
    }

    @Override
    public String toString() {
        return DbName;
    }

    public static String DbName()
    {
        return DbName;
    }

    @Override
    public void close() throws Exception
    {
        driver.close();
    }
}
