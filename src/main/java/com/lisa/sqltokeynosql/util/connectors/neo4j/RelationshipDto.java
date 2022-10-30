package com.lisa.sqltokeynosql.util.connectors.neo4j;

import java.util.List;

public class RelationshipDto
{
    public final List<StringBuilder> match;
    public final List<StringBuilder> create;
    public final List<String> columnsFks;

    public RelationshipDto(
            List<StringBuilder> match,
            List<StringBuilder> create,
            List<String> columnsFks)
    {
        this.match = match;
        this.create = create;
        this.columnsFks = columnsFks;
    }
}
