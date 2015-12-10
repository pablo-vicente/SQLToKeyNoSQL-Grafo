package util;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.policy.actions.SimpleDBActions;
import com.amazonaws.services.simpledb.AmazonSimpleDBClient;
import com.amazonaws.services.simpledb.model.Attribute;
import com.amazonaws.services.simpledb.model.CreateDomainRequest;
import com.amazonaws.services.simpledb.model.GetAttributesRequest;
import com.amazonaws.services.simpledb.model.ListDomainsRequest;
import com.amazonaws.services.simpledb.model.ListDomainsResult;
import com.amazonaws.services.simpledb.model.PutAttributesRequest;
import com.amazonaws.services.simpledb.model.ReplaceableAttribute;
import com.lisa.sqltokeynosql.architecture.Connector;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 *
 * @author geomar
 */
public class SimpleDBConnector extends Connector {

    AmazonSimpleDBClient client;
    protected final static String AWSAccessKeyID = "AKIAJA6BCQHAOANBHHPQ";
    protected final static String AWSSecretKey = "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";
    String ndb;

    @Override
    public void connect(String nbd) {
        this.ndb = ndb;
        this.client = new AmazonSimpleDBClient(new BasicAWSCredentials(AWSAccessKeyID, AWSSecretKey), new ClientConfiguration());
    }

    @Override
    public void put(String table, String key, ArrayList<String> cols, ArrayList<String> values) {

        if (null == this.client) {
            System.err.println("Problemas na conexão com o SimpleDB");
            //this.client.setEndpoint("sdb.amazonaws.com");
            return;
        }
        if (!doesDomainExist(table))
            this.client.createDomain(new CreateDomainRequest(table));

        ArrayList<ReplaceableAttribute> attributes = new ArrayList<ReplaceableAttribute>();
        ReplaceableAttribute att;
        for (int i = 0; i < cols.size(); i++) {
            att = new ReplaceableAttribute(cols.get(i), values.get(i), Boolean.TRUE);
            attributes.add(att);
        }
        this.client.putAttributes(new PutAttributesRequest(table, key, attributes));
    }

    protected List<String> getAllDomains() {
        List<String> domains = new ArrayList<String>(1000);
        String nextToken = null;
        do {
            ListDomainsRequest ldr = new ListDomainsRequest();
            ldr.setNextToken(nextToken);

            ListDomainsResult result = this.client.listDomains(ldr);
            domains.addAll(result.getDomainNames());

            nextToken = result.getNextToken();
        } while (nextToken != null);

        return domains;
    }

    protected boolean doesDomainExist(String domainName) {
        try {
            List<String> domains = this.getAllDomains();
            return (domains.contains(domainName));
        } catch (Exception exception) {
            //log.log(Level.WARNING, "Exception during doesDomainExist", exception);
            return false;
        }
    }

    @Override
    public void delete() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public HashMap<String, String> get(int n, String t, final String key) {
        if (key.isEmpty()) {
            System.err.println("Chave veio vazia!");
            return null;
        }
        HashMap<String, String> result = new HashMap();

        GetAttributesRequest gar = new GetAttributesRequest(t, key).withConsistentRead(Boolean.TRUE);
        System.out.println(">>" + gar.toString());
        List<Attribute> data = this.client.getAttributes(gar).getAttributes();
        for (Attribute att : data) {
            result.put(att.getName(), att.getValue());
        }
        return result;
    }

    @Override
    public String toString() {
        return "SimpleDB";
    }

}
