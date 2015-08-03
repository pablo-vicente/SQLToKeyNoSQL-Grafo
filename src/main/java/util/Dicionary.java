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
    
    
    
}
