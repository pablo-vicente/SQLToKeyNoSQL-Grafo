package com.lisa.sqltokeynosql.util.connectors;

import com.lisa.sqltokeynosql.architecture.Connector;
import com.lisa.sqltokeynosql.util.sql.Table;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author geomar
 */
public class Cassandra2Connector extends Connector {

    private AstyanaxContext<Keyspace> context;
    Keyspace keyspace;

    @Override
    public void connect(String nameDB) {
        this.context = new AstyanaxContext.Builder()
                .forCluster("ClusterName")
                .forKeyspace(nameDB)
                .withAstyanaxConfiguration(new AstyanaxConfigurationImpl()
                        .setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE)
                )
                .withConnectionPoolConfiguration(new ConnectionPoolConfigurationImpl("MyConnectionPool")
                        .setPort(9160)
                        .setMaxConnsPerHost(1)
                        .setSeeds("127.0.0.1:9160")
                )
                .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
                .buildKeyspace(ThriftFamilyFactory.getInstance());
        this.context.start();
        this.keyspace = this.context.getClient();

    }

    @Override
    public void put(Table table, List<String> cols, Map<String, List<String>> dados)
    {
        ColumnFamily<String, String> TABLE
                = new ColumnFamily<String, String>(
                        table.getName(), // Column Family Name
                        StringSerializer.get(), // Key Serializer
                        StringSerializer.get());

        MutationBatch m = keyspace.prepareMutationBatch();

        for (Map.Entry<String, List<String>> stringListEntry : dados.entrySet())
        {
            var key = stringListEntry.getKey();
            var values = stringListEntry.getValue();
            for (int i = 0; i < cols.size(); i++) {
                m.withRow(TABLE, key).putColumn(cols.get(i), values.get(i), null);
            }
            try {
                OperationResult<Void> result = m.execute();
            } catch (ConnectionException e) {
                System.err.println("Problemas conxão Cassandra!!");
            }
        }

    }

    @Override
    public void delete(String t, String...keys) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public HashMap<String, String> get(int n, String t, String key) {
        HashMap<String, String> _new = new HashMap<String, String>();
        ColumnFamily<String, String> TABLE
                = new ColumnFamily<String, String>(
                        t, // Column Family Name
                        StringSerializer.get(), // Key Serializer
                        StringSerializer.get());

        
        OperationResult<ColumnList<String>> result = null;
        try {
            result = this.keyspace.prepareQuery(TABLE)
                    .getKey(key)
                    .execute();
        } catch (ConnectionException ex) {
            Logger.getLogger(Cassandra2Connector.class.getName()).log(Level.SEVERE, null, ex);
        }
        if (result == null){
            System.out.println("Row not Found!");
            return null;
        }
        ColumnList<String> columns = result.getResult();

        for (Column<String> c : result.getResult()) {
            _new.put(c.getName(), c.getStringValue());
        }
        return _new;
    }

    @Override
    public String toString() {
        return "Cassandra Net";
    }    
}
