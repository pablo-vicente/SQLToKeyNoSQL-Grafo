package com.lisa.sqltokeynosql.util.connectors.neo4j;

import java.util.ArrayList;
import java.util.List;

public class VerifyRelationshipDto
{
    public final ArrayList<StringBuilder> querysRelationships;
    public final ArrayList<StringBuilder> queriesVerifyFks;
    public final List<String> columnsFks;

    public VerifyRelationshipDto(ArrayList<StringBuilder> querysRelationships, ArrayList<StringBuilder> queriesVerifyFks, List<String> columnsFks) {
        this.querysRelationships = querysRelationships;
        this.queriesVerifyFks = queriesVerifyFks;
        this.columnsFks = columnsFks;
    }
}