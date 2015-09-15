/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package util;

import java.util.ArrayList;

/**
 *
 * @author geomar
 */
public class Dicionary {

    private String current_db = null;
    ArrayList<BDR> Bdrs;
    ArrayList<NoSQL> targets;

    public Dicionary() {
        this.Bdrs = new <BDR> ArrayList();
        this.targets = new <NoSQL> ArrayList();
    }

    public ArrayList<BDR> getBdrs() {
        return Bdrs;
    }

    public void setBdrs(ArrayList<BDR> Bdrs) {
        this.Bdrs = Bdrs;
    }

    public ArrayList<NoSQL> getTargets() {
        return targets;
    }

    public void setTargets(ArrayList<NoSQL> targets) {
        this.targets = targets;
    }

    public BDR getRBD(String dbn) {
        if (dbn != null) {
            for (BDR db : Bdrs) {
                if (dbn.equals(db.getName())) {
                    return db;
                }
            }
        }
        return null;
    }

    public NoSQL getTarget(String alias) {
        if (alias == null) {
            return targets.get(0);
        }

        for (NoSQL t : targets) {
            if (t.getAlias().equals(alias)) {
                return t;
            }
        }
        return null;
    }

    public String getCurrent_db() {
        return current_db;
    }

    public void setCurrent_db(String current_db) {
        this.current_db = (current_db);
    }

}
