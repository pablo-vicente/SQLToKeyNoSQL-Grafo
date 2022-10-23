using GeradorSQL.Enums;

namespace GeradorSQL.Seeds;

public static class SeedsFactory
{
    public static async Task GenerateAsync(Consulta consulta, long linhas, StreamWriter streamWriter)
    {
        switch (consulta)
        {
            case Consulta.Insert:
                await GenerateAsync(Inserts, linhas, streamWriter).ConfigureAwait(false);
                break;
            case Consulta.InsertN:
                await GenerateInsertsAsync(linhas, streamWriter).ConfigureAwait(false);
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(consulta), consulta, null);
        }
    }
    
    public static long Count(Consulta consulta)
    {
        return consulta switch
        {
            Consulta.InsertN => InsertsN.Count,
            Consulta.Insert => Inserts.Count,
            _ => throw new ArgumentOutOfRangeException(nameof(consulta), consulta, null)
        };
    }
    
    private static async Task GenerateInsertsAsync(long linhas, TextWriter streamWriter)
    {
        var seeds = InsertsN;
        
        foreach (var seed in seeds)
        {
            var (parte1, parte2) = seed;
            await streamWriter.WriteLineAsync(parte1).ConfigureAwait(true);
            
            for (var id = 1; id <= linhas; id++)
            {
                var final = id == linhas ? ";" : ",";
                var seedAustada = parte2.Replace(Utils.Substituir, id.ToString()) + final;
                await streamWriter.WriteLineAsync(seedAustada).ConfigureAwait(true);
            }
        }
    }
    
    private static async Task GenerateAsync(IEnumerable<string> seeds, long linhas, TextWriter streamWriter)
    {
        foreach (var seed in seeds)
        {
            for (var id = 1; id <= linhas; id++)
            {
                var seedAustada = seed.Replace(Utils.Substituir, id.ToString());
                await streamWriter.WriteLineAsync(seedAustada).ConfigureAwait(true);
            }
        }
    }
    
     private static readonly IList<string> Inserts = new List<string>
    {
        $"INSERT INTO cliente(cli_id,Nome,data_nasc,telefone,email,RG,CPf,endereco,razao_social,t_Pessoa,cep,cidade,bairro,uf) VALUES ({Utils.Substituir},'Lucas Oliveira','1998-04-23','(19)3333-3333','Lucas@com.net','45.343.942-1','450.802.308.86','Rua: vinicius de Moraes n°846','1111','pessoa fisica','13.188-103','Hortolândia','Jd.Amanda','sp');",
        $"INSERT INTO fornecedor (for_id,Nome,data_nasc,telefone,email,site,RG,CPf,endereco,razao_social,t_Pessoa,cnpj,cep,ins_Est,cidade,bairro,uf) VALUES ({Utils.Substituir},'Queiroz','2000-02-02','(19)9999-6666','kkj','jkjk','45.444.444-4','444.444.444-44','j','ftfy','Pesso juridica','12','12.111-111','kjk','hi','jd','SP');",
        $"INSERT INTO veiculo (id_veiculo,desc_veiculo,ano_veiculo,placa_veiculo) VALUES ({Utils.Substituir},'Caminhao mercedes','2017','abc-6666');",
        $"INSERT INTO funcao(id_funcao, desc_funcao,salario,carga_horaria) VALUES ({Utils.Substituir},'administrador',2000.00,'40 horas semanais');",
        $"INSERT INTO Banco (id_banco, desc_banco) VALUES ({Utils.Substituir},'Bradesco');",
        $"INSERT INTO fluxo_caixa (id, vl_pd, vl_nd, vl_pm, vl_nm, contAreceber, contApagar) VALUES ({Utils.Substituir}, 2000, 3000, 4000, 5000, 2600, 10000);",
        $"INSERT INTO Fabricante(id_fabricante, nome_fabricante,marca_fabricante) VALUES ({Utils.Substituir},'Tigre ltda','tigre');",
        $"INSERT INTO Setor(id_Setor, setor_produto,sub_setor_produto,tipo) VALUES ({Utils.Substituir}, 'Hidraulica','valvula','Premium');",
        $"INSERT INTO Localizacao_Prod(id_loc,Corredor,partilhera,gaveta) VALUES ({Utils.Substituir},'1a','1a','1a');",
        $"INSERT INTO Forma_pag (id_formaPag,desc_formPag) VALUES ({Utils.Substituir},'Dinheiro');",
        $"INSERT INTO funcionario (id, funcao_id,banco_id,Nome,data_nasc,telefone,email,t_pessoa,razao_social,RG,CPf,endereco,cep,cnh,cat,ctps,bairro,cidade,uf,agencia,conta) VALUES ({Utils.Substituir},{Utils.Substituir},{Utils.Substituir},'Lucas Oliveira','1998-04-23','(19)3333-3333','Lucas@com.net','Pessoa Física','num sei cara','45.343.942-1','450.802.308.86','Rua: vinicius de Moraes n°846','13.188-103','1111','2222','3333','Jd.Amanda','Hortolândia','sp','4444','5555');",
        $"INSERT INTO usuario (user_id, func_id, user_log, user_pwd) VALUES ({Utils.Substituir},{Utils.Substituir},'adm','adm');",
        $"INSERT INTO Produto (id_produto, id_setor,id_fabricante,cod_barra,descricao,marca,unidade_med,preco_compra,preco_venda,margem_lucro) VALUES ({Utils.Substituir},{Utils.Substituir},{Utils.Substituir},12345678,'Torneira','tigre','unid',10,20,100);",
        $"INSERT INTO Item_venda (id_Item,id_produto,quant_vendida, prec_unitario) VALUES ({Utils.Substituir},{Utils.Substituir},1,100);",
        $"INSERT INTO estoque (id_estoque,id_loc,for_id,id_produto,quant_disponivel,dataADD) VALUES ({Utils.Substituir},{Utils.Substituir},{Utils.Substituir},{Utils.Substituir},100,'2015-07-16');",
        $"INSERT INTO vendas (id_venda,fun_id,id_itemVenda,id_formaPag,valor_venda, valor_recebido, troco, data_venda, entrega) VALUES ({Utils.Substituir},{Utils.Substituir},{Utils.Substituir},{Utils.Substituir},1000, 2000, 1000, '2004-05-23T14:25:10', '23/05/1998');",
        $"INSERT INTO entrega (id_entrega, id_venda, id_veiculo, rua, numero, bairro, cidade, uf, cep) VALUES ({Utils.Substituir}, {Utils.Substituir}, {Utils.Substituir}, 'Rua', 'Numero', 'bairro', 'cidade', 'uf', 'cep');"
    };

    private static readonly IList<(string, string)> InsertsN = new List<(string, string)>
    {
        ("INSERT INTO cliente(cli_id,Nome,data_nasc,telefone,email,RG,CPf,endereco,razao_social,t_Pessoa,cep,cidade,bairro,uf) VALUES ", $"({Utils.Substituir},'Lucas Oliveira','1998-04-23','(19)3333-3333','Lucas@com.net','45.343.942-1','450.802.308.86','Rua: vinicius de Moraes n°846','1111','pessoa fisica','13.188-103','Hortolândia','Jd.Amanda','sp')"),
        ("INSERT INTO fornecedor (for_id,Nome,data_nasc,telefone,email,site,RG,CPf,endereco,razao_social,t_Pessoa,cnpj,cep,ins_Est,cidade,bairro,uf) VALUES ",$"({Utils.Substituir},'Queiroz','2000-02-02','(19)9999-6666','kkj','jkjk','45.444.444-4','444.444.444-44','j','ftfy','Pesso juridica','12','12.111-111','kjk','hi','jd','SP')"),
        ("INSERT INTO veiculo (id_veiculo,desc_veiculo,ano_veiculo,placa_veiculo) VALUES ",$"({Utils.Substituir},'Caminhao mercedes','2017','abc-6666')"),
        ("INSERT INTO funcao(id_funcao, desc_funcao,salario,carga_horaria) VALUES", $"({Utils.Substituir},'administrador',2000.00,'40 horas semanais')"),
        ("INSERT INTO Banco (id_banco, desc_banco) VALUES ", $"({Utils.Substituir},'Bradesco')"),
        ("INSERT INTO fluxo_caixa (id, vl_pd, vl_nd, vl_pm, vl_nm, contAreceber, contApagar) VALUES ", $"({Utils.Substituir}, 2000, 3000, 4000, 5000, 2600, 10000)"),
        ("INSERT INTO Fabricante(id_fabricante, nome_fabricante,marca_fabricante) VALUES ", $"({Utils.Substituir},'Tigre ltda','tigre')"),
        ("INSERT INTO Setor(id_Setor, setor_produto,sub_setor_produto,tipo) VALUES ", $"({Utils.Substituir}, 'Hidraulica','valvula','Premium')"),
        ("INSERT INTO Localizacao_Prod(id_loc,Corredor,partilhera,gaveta) VALUES", $"({Utils.Substituir},'1a','1a','1a')"),
        ("INSERT INTO Forma_pag (id_formaPag,desc_formPag) VALUES ", $"({Utils.Substituir},'Dinheiro')"),
        ("INSERT INTO funcionario (id, funcao_id,banco_id,Nome,data_nasc,telefone,email,t_pessoa,razao_social,RG,CPf,endereco,cep,cnh,cat,ctps,bairro,cidade,uf,agencia,conta) VALUES", $"({Utils.Substituir},{Utils.Substituir},{Utils.Substituir},'Lucas Oliveira','1998-04-23','(19)3333-3333','Lucas@com.net','Pessoa Física','num sei cara','45.343.942-1','450.802.308.86','Rua: vinicius de Moraes n°846','13.188-103','1111','2222','3333','Jd.Amanda','Hortolândia','sp','4444','5555')"),
        ("INSERT INTO usuario (user_id, func_id, user_log, user_pwd) VALUES ", $"({Utils.Substituir},{Utils.Substituir},'adm','adm')"),
        ("INSERT INTO Produto (id_produto, id_setor,id_fabricante,cod_barra,descricao,marca,unidade_med,preco_compra,preco_venda,margem_lucro) VALUES", $"({Utils.Substituir},{Utils.Substituir},{Utils.Substituir},12345678,'Torneira','tigre','unid',10,20,100)"),
        ("INSERT INTO Item_venda (id_Item,id_produto,quant_vendida, prec_unitario) VALUES ", $"({Utils.Substituir},{Utils.Substituir},1,100)"),
        ("INSERT INTO estoque (id_estoque,id_loc,for_id,id_produto,quant_disponivel,dataADD) VALUES ", $"({Utils.Substituir},{Utils.Substituir},{Utils.Substituir},{Utils.Substituir},100,'2015-07-16')"),
        ("INSERT INTO vendas (id_venda,fun_id,id_itemVenda,id_formaPag,valor_venda, valor_recebido, troco, data_venda, entrega) VALUES ", $"({Utils.Substituir},{Utils.Substituir},{Utils.Substituir},{Utils.Substituir},1000, 2000, 1000, '2004-05-23T14:25:10', '23/05/1998')"),
        ("INSERT INTO entrega (id_entrega, id_venda, id_veiculo, rua, numero, bairro, cidade, uf, cep) VALUES ", $"({Utils.Substituir}, {Utils.Substituir}, {Utils.Substituir}, 'Rua', 'Numero', 'bairro', 'cidade', 'uf', 'cep')")
    };

}

