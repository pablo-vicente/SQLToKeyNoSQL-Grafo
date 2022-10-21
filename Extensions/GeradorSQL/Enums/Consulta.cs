using System.ComponentModel.DataAnnotations;

namespace GeradorSQL.Enums;

public enum Consulta
{
    [Display(Name = "INSERT N")]
    InsertN = 1,
    
    [Display(Name = "INSERT")]
    Insert = 2,
    
    [Display(Name = "UPDATE (Todas as Colunas Todos Registros")]
    Update = 3,
    
    // [Display(Name = "UPDATE SEM CHAVES ESTRANGEIRAS")]
    // UpdateSimples = 4,
    //
    // [Display(Name = "UPDATE DE CHAVES ESTRANGEIRAS")]
    // UpdateChavesEstrangeiras = 5,
}