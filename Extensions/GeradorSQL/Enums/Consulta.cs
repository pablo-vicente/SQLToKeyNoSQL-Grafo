using System.ComponentModel.DataAnnotations;

namespace GeradorSQL.Enums;

public enum Consulta
{
    [Display(Name = "INSERT N")]
    InsertN = 1,
    
    [Display(Name = "INSERT")]
    Insert = 2
}