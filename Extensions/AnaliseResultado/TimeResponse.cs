namespace AnaliseResultado;

public record struct TimeResponse
{
    public decimal TempoCamada { get; set; }
    public decimal TempoConector { get; set; }
    public decimal TempoNeo4j { get; set; }
}