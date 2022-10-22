using GeradorSQL.Enums;
using GeradorSQL.Seeds;

namespace GeradorSQL.Services;

public static class ConsultasService
{
    public static async Task<FileInfo> GerarAsync(Consulta tipoConsulta, int linhas)
    {
        var basePath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Consultas");
        if (!Directory.Exists(basePath))
            Directory.CreateDirectory(basePath);
        
        var sufixo = tipoConsulta + "_" + linhas * SeedsFactory.Count(tipoConsulta);
        var file = new FileInfo(Path.Combine(basePath, $"{sufixo}.sql"));
    
        var streamWriter = new StreamWriter(file.FullName);
        
        await SeedsFactory
            .GenerateAsync(tipoConsulta, linhas, streamWriter)
            .ConfigureAwait(false);

        await streamWriter.FlushAsync();

        return file;
    }
}