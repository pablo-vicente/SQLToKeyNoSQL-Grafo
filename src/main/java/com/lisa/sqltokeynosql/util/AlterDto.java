package com.lisa.sqltokeynosql.util;

import net.sf.jsqlparser.statement.alter.AlterOperation;

public class AlterDto
{
    public AlterDto(AlterOperation alterOperation, String colunaExistente, String colunaNova, Boolean chaveEstrangeira)
    {
        AlterOperation = alterOperation;
        ColunaExistente = colunaExistente;
        ColunaNova = colunaNova;
        ChaveEstrangeira = chaveEstrangeira;
    }

    public final AlterOperation AlterOperation;
    public final String ColunaExistente;
    public final String ColunaNova;
    public final Boolean ChaveEstrangeira;
}
