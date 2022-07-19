package com.lisa.sqltokeynosql.util.connectors;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.simpledb.AmazonSimpleDBClient;
import com.amazonaws.services.simpledb.model.Attribute;
import com.amazonaws.services.simpledb.model.DeleteAttributesRequest;
import com.amazonaws.services.simpledb.model.GetAttributesRequest;
import com.amazonaws.services.simpledb.model.Item;
import com.amazonaws.services.simpledb.model.ListDomainsRequest;
import com.amazonaws.services.simpledb.model.ListDomainsResult;
import com.amazonaws.services.simpledb.model.PutAttributesRequest;
import com.amazonaws.services.simpledb.model.ReplaceableAttribute;
import com.amazonaws.services.simpledb.model.SelectRequest;
import com.amazonaws.services.simpledb.model.SelectResult;
import com.lisa.sqltokeynosql.architecture.Connector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Stack;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.schema.Column;
import com.lisa.sqltokeynosql.util.operations.Equal;
import com.lisa.sqltokeynosql.util.operations.Greater;
import com.lisa.sqltokeynosql.util.operations.GreaterEqual;
import com.lisa.sqltokeynosql.util.operations.Minor;
import com.lisa.sqltokeynosql.util.operations.MinorEqual;
import com.lisa.sqltokeynosql.util.operations.Operator;

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
    public void connect(String nameDB) {
        this.ndb = ndb;
        this.client = new AmazonSimpleDBClient(new BasicAWSCredentials(AWSAccessKeyID, AWSSecretKey), new ClientConfiguration());
    }

    @Override
    public void put(String table, String key, LinkedList<String> cols, ArrayList<String> values) {

        if (null == this.client) {
            System.err.println("Problemas na conexão com o SimpleDB");
            //this.client.setEndpoint("sdb.amazonaws.com");
            return;
        }
        //if (!doesDomainExist(table)) {
        //  this.client.createDomain(new CreateDomainRequest(table));
        //}

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
    public void delete(String t, String key) {
        DeleteAttributesRequest del = new DeleteAttributesRequest(t, key);
        this.client.deleteAttributes(del);
    }

    @Override
    public HashMap<String, String> get(int n, String t, final String key) {
        if (key.isEmpty()) {
            System.err.println("Chave veio vazia!");
            return null;
        }
        HashMap<String, String> result = new HashMap();

        GetAttributesRequest gar = new GetAttributesRequest(t, key).withConsistentRead(Boolean.TRUE);
        List<Attribute> data = this.client.getAttributes(gar).getAttributes();
        for (Attribute att : data) {
            result.put(att.getName(), att.getValue());
        }
        result.put("_key", key);
        return result;
    }

    public ArrayList<HashMap<String, String>> getN(int n, String t, ArrayList<String> keys) {
        //if (keys != null && keys.isEmpty()) {
        //   System.err.println("Chave veio vazia!");
        //   return null;
        // }
        ArrayList<HashMap<String, String>> result = new ArrayList();
        HashMap<String, String> aux;
        SelectRequest s = new SelectRequest("SELECT * FROM " + t + " LIMIT 2500");
        SelectResult sr = null;
        sr = this.client.select(s);
        for (Item item : sr.getItems()) {
            String it = item.getName();
            aux = new HashMap<>();
            List<Attribute> attr = item.getAttributes();
            for (Attribute a : attr) {
                aux.put(a.getName(), a.getValue());
            }
            aux.put("_key", it);
            result.add(aux);
        }
        String token = sr.getNextToken();
        while (token != null) {
            s.setNextToken(token);
            sr = this.client.select(s);
            for (Item item : sr.getItems()) {
                String it = item.getName();
                aux = new HashMap<>();
                List<Attribute> attr = item.getAttributes();
                for (Attribute a : attr) {
                    aux.put(a.getName(), a.getValue());
                }
                aux.put("_key", it);
                result.add(aux);
            }
            if (sr.getItems().size() <= 3) {
                token = null;
            } else {
                token = sr.getNextToken();
            }

        }
        return result;
    }

    @Override
    public ArrayList getN(int n, String t, ArrayList<String> keys, Stack<Object> filters, LinkedList<String> cols) {
        ArrayList<String[]> result = new ArrayList<String[]>();
        String[] aux;
        String s_cols = cols.toString().replace("[", "").replace("]", "");
        SelectRequest s;
        filters = this.resumeFilter(filters, t);
       if (filters == null)
           s = new SelectRequest("SELECT "+s_cols+" FROM " + t + " LIMIT 2500");
       else
              s = new SelectRequest("SELECT "+s_cols+" FROM " + t + " WHERE "+this.filter(filters)+" LIMIT 2500");
        
        SelectResult sr = null;
        sr = this.client.select(s);
        for (Item item : sr.getItems()) {
            String it = item.getName();
            aux = new String[cols.size()];
            List<Attribute> attr = item.getAttributes();
            for (Attribute a : attr) {
                if (cols.contains(a.getName())) {
                    aux[cols.indexOf(a.getName())] = a.getValue();
                }
            }
            result.add(aux);
        }
        String token = sr.getNextToken();
        while (token != null) {
            s.setNextToken(token);
            sr = this.client.select(s);
            for (Item item : sr.getItems()) {
                String it = item.getName();
                aux = new String[cols.size()];
                List<Attribute> attr = item.getAttributes();
                for (Attribute a : attr) {
                    if (cols.contains(a.getName())) {
                        aux[cols.indexOf(a.getName())] = a.getValue();
                    }
                }
                result.add(aux);
            }
            if (sr.getItems().size() <= 3) {
                token = null;
            } else {
                token = sr.getNextToken();
            }

        }
        return result;
    }

    private String filter(Stack filters) {
        if (filters == null) {
            return "";
        }

        String result = "";
        //BSON result = new Document();
        Object o = filters.pop();
        if (o instanceof AndExpression) {
            result = filter(filters) + " AND " + filter(filters);

            //  result = (result && applyFilterR(filters, tuple));
            //    } else if (o instanceof OrExpression) {
            //          result = applyFilterR(filters, tuple);
            //         result = (result || applyFilterR(filters, tuple));
        } else {
            String col = null;
            Object val = null;
            Operator op = null;
            op = ((Operator) o);
            val = filters.pop().toString();
            col = ((Column) filters.pop()).getColumnName();
            if (o instanceof Equal) {
                result = col + " = " + val;
            } else if (o instanceof Greater) {
                result = col + " > " + val;
            } else if (o instanceof Minor) {
                result = col + " < " + val;
            } else if (o instanceof GreaterEqual) {
                result = col + " >= " + val;
            } else if (o instanceof MinorEqual) {
                result = col + " <= " + val;
            }
        }
        return result;
    }

    private Stack<Object> resumeFilter(Stack<Object> filters, String table) {
        Stack<Object> resumed_filters = null;
        Object t = filters.pop();

        if (t instanceof AndExpression) {
           resumed_filters = resumeFilter(filters, table);
           if (resumed_filters == null){
               return resumeFilter(filters, table);
           }else{
               Stack<Object> aux = resumeFilter(filters, table);
               for (int i=0; i<aux.size();i++)
                   resumed_filters.add(aux.get(i));
               resumed_filters.add(t);
           }
        }else 
        if (t instanceof Operator) {
            Operator o = (Operator) t;
            //Object val = 
            Object val = filters.pop();
            Column tab = (Column) filters.pop();
            if (tab.getTable().getName() == null || tab.getTable().getName().equals(table)) {
                //if (filters.empty()) {
                    resumed_filters = new Stack();
                //} else {
                 //   resumed_filters = resumeFilter(filters, table);
                  //  if (resumed_filters == null) {
                   //     new Stack();
                    //}
                //}
                resumed_filters.add(tab);
                resumed_filters.add(val);
                resumed_filters.add(o);
            }

        }
        return resumed_filters;
    }

    @Override
    public String toString() {
        return "SimpleDB";
    }

}
