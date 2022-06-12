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
    public void put(String table, String key, LinkedList<String> cols, ArrayList<String> values)
    {
        // TODO IMPLEMENTAT SEM PK
        // TODO IMPLEMENTAR COM PK

        try (Session session = driver.session(SessionConfig.forDatabase(_nomeBancoDados)))
        {

            String queryId =  getQueryById(table, _idColumnName, key);
            List<Record> results = session.run(queryId).list();

            if(results.size() >= 1)
                throw new UnsupportedOperationException("Duplicate register for id " + key);

            String queryInsert = getQueryInsert(table, key, cols, values);

            session.run(queryInsert);
        }
        catch (Exception exception)
        {
            System.out.println(exception);
            throw exception;
        }

    }

    private String getQueryById(String table, String atribute, String value)
    {
        return "Match (n:"+ table + ") Where n." + atribute + "=" + value + " return n";
    }

    private String getQueryInsert(String table, String key, LinkedList<String> cols, ArrayList<String> values)
    {
        boolean contaisId = false;

        String atributes = "";
        for (int i = 0; i < cols.size(); i++)
        {
            String name = cols.get(i);
            String value = values.get(i);
            atributes += name + ":" + value + ",";

            if(_idColumnName.equalsIgnoreCase(name))
                contaisId = true;
        }

        if(!contaisId)
            atributes += _idColumnName + ":" + key + "";
        else
            atributes = atributes.substring(0, atributes.length() -1);


        String query = "CREATE (n:" + table +
                "{" +
                atributes +
                "})";

        return query;
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
