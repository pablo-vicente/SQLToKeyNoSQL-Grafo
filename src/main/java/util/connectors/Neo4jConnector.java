/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package util.connectors;


import java.util.*;

import com.lisa.sqltokeynosql.architecture.Connector;
import org.neo4j.driver.*;

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

    public Neo4jConnector()
    {
        String uri = "bolt://localhost:7687";
        String user = "neo4j";
        String password = "Neo4j";
        driver = GraphDatabase.driver( uri, AuthTokens.basic( user, password ) );
    }

    public void printGreeting( final String message )
    {
        try ( Session session = driver.session(SessionConfig.forDatabase(_nomeBancoDados)) )
        {
            String greeting = session.writeTransaction( tx ->
            {
                Result result = tx.run( "CREATE (a:Greeting) " +
                                "SET a.message = $message " +
                                "RETURN a.message + ', from node ' + id(a)",
                        parameters( "message", message ) );
                return result.single().get( 0 ).asString();
            } );
            System.out.println( greeting );
        }
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
    public void put(String table, String key, LinkedList<String> cols, ArrayList<String> values) {

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
