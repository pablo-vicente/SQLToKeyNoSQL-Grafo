/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package util;

/**
 *
 * @author geomar
 */
public class NoSQL {
    private String alias;
    private String user;
    private String password;
    private String url;
    private ConectorMongo conection;

    public NoSQL(String alias, String user, String password, String url) {
        this.alias = alias;
        this.user = user;
        this.password = password;
        this.url = url;
        conection = new ConectorMongo();
        conection.connect(alias);
    }

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public ConectorMongo getConection() {
        return conection;
    }

    public void setConection(ConectorMongo conection) {
        this.conection = conection;
    }
    
}
