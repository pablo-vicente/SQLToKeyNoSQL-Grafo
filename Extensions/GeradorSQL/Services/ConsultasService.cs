using GeradorSQL.Enums;

namespace GeradorSQL.Services;

public static class ConsultasService
{
    public static async Task GerarAsync(Consulta tipoConsulta, int linhas, StreamWriter streamWriter)
    {
        await Seeds
            .SeedsFactory
            .GenerateAsync(tipoConsulta, linhas, streamWriter)
            .ConfigureAwait(false);

    }
}