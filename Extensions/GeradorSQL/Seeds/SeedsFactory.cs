using GeradorSQL.Enums;

namespace GeradorSQL.Seeds;

public static class Seeds
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
            // case Consulta.UpdateSimples:
            //     await GenerateAsync(UpdateSemChaveEstrangeira, linhas, streamWriter).ConfigureAwait(false);
            //     break;
            // case Consulta.UpdateChavesEstrangeiras:
            //     await GenerateAsync(UpdateComChaveEstrangeira, linhas, streamWriter).ConfigureAwait(false);
            //     break;
            case Consulta.Update:
                await GenerateAsync(Updates, linhas, streamWriter).ConfigureAwait(false);
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
            // Consulta.UpdateSimples => UpdateSemChaveEstrangeira.Count,
            // Consulta.UpdateChavesEstrangeiras => UpdateComChaveEstrangeira.Count,
            Consulta.Update => Updates.Count,
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
    
    private static async Task GenerateAsync(IEnumerable<string> seeds, long linhas, StreamWriter streamWriter)
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

    private static readonly IList<string> Updates = new List<string>
    {
        $"UPDATE cliente SET Nome='Lucas Oliveira (Atualizado)', data_nasc='1998-04-23', telefone='(19)3333-3333', email='Lucas@com.net', RG='45.343.942-1', CPf='450.802.308.86', endereco='Rua: vinicius de Moraes n°846', razao_social='1111', t_Pessoa='pessoa fisica', cep='13.188-103', cidade='Hortolândia', bairro='Jd.Amanda', uf='sp' WHERE cli_id={Utils.Substituir};",
        $"UPDATE fornecedor SET Nome='Queiroz (Atualizado)', data_nasc='2000-02-02', telefone='(19)9999-6666', email='kkj', site='jkjk', RG='45.444.444-4', CPf='444.444.444-44', endereco='j', razao_social='ftfy', t_Pessoa='Pesso juridica', cnpj='12', cep='12.111-111', ins_Est='kjk', cidade='hi', bairro='jd', uf='SP' WHERE for_id={Utils.Substituir};",
        $"UPDATE veiculo SET desc_veiculo='Caminhao mercedes (Atualizado)', ano_veiculo='2017', placa_veiculo='abc-6666' WHERE id_veiculo={Utils.Substituir};",
        $"UPDATE funcao SET desc_funcao='administrador (Atualizado)', salario=2000.00, carga_horaria='40 horas semanais' WHERE id_funcao={Utils.Substituir};",
        $"UPDATE Banco SET desc_banco='Bradesco (Atualizado)' WHERE id_banco={Utils.Substituir};",
        $"UPDATE fluxo_caixa SET vl_pd=4000, vl_nd=3000, vl_pm=4000, vl_nm=5000, contAreceber=2600, contApagar=10000 WHERE id={Utils.Substituir};",
        $"UPDATE Fabricante SET nome_fabricante='Tigre ltda (Atualizado)', marca_fabricante='tigre' WHERE id_fabricante={Utils.Substituir};",
        $"UPDATE Setor SET setor_produto='Hidraulica (Atualizado)', sub_setor_produto='valvula', tipo='Premium' WHERE id_Setor={Utils.Substituir};",
        $"UPDATE Localizacao_Prod SET Corredor='1a (Atualizado)', partilhera='1a', gaveta='1a' WHERE id_loc={Utils.Substituir};",
        $"UPDATE Forma_pag SET desc_formPag='Dinheiro (Atualizado)' WHERE id_formaPag={Utils.Substituir};",
        $"UPDATE funcionario SET Nome='Lucas Oliveira (Atualizado)', funcao_id={Utils.Substituir}, banco_id={Utils.Substituir}, data_nasc='1998-04-23', telefone='(19)3333-3333', email='Lucas@com.net', t_pessoa='Pessoa Física', razao_social='num sei cara', RG='45.343.942-1', CPf='450.802.308.86', endereco='Rua: vinicius de Moraes n°846', cep='13.188-103', cnh='1111', cat='2222', ctps='3333', bairro='Jd.Amanda', cidade='Hortolândia', uf='sp', agencia='4444', conta='5555' WHERE id={Utils.Substituir};",
        $"UPDATE usuario SET func_id={Utils.Substituir}, user_log='adm (Atualizado)', user_pwd='adm' WHERE user_id={Utils.Substituir};",
        $"UPDATE Produto SET id_setor={Utils.Substituir}, id_fabricante={Utils.Substituir}, cod_barra=12345678, descricao='Torneira (Atualizado)', marca='tigre', unidade_med='unid', preco_compra=10, preco_venda=20, margem_lucro=100 WHERE id_produto={Utils.Substituir};",
        $"UPDATE Item_venda SET id_produto={Utils.Substituir}, quant_vendida=1, prec_unitario=100 WHERE id_Item={Utils.Substituir};",
        $"UPDATE estoque SET id_loc={Utils.Substituir}, for_id={Utils.Substituir}, id_produto={Utils.Substituir}, quant_disponivel=100, dataADD='2015-07-16 (Atualizado)' WHERE id_estoque={Utils.Substituir};",
        $"UPDATE vendas SET fun_id={Utils.Substituir}, id_itemVenda={Utils.Substituir}, id_formaPag={Utils.Substituir}, valor_venda=1000, valor_recebido=2000, troco=1000, data_venda='2004-05-23T14:25:10', entrega='23/05/1998 (Atualizado)' WHERE id_venda={Utils.Substituir};",
        $"UPDATE entrega SET id_venda={Utils.Substituir}, id_veiculo={Utils.Substituir}, rua='Rua (Atualizado)', numero='Numero', bairro='bairro', cidade='cidade', uf='uf', cep='cep' WHERE id_entrega={Utils.Substituir};"
    };

    #region Updates Seeds

    // private static readonly IList<string> UpdateSemChaveEstrangeira = new List<string>
    // {
    //     $"UPDATE cliente SET Nome = 'Lucas Oliveira (Atualizado)' WHERE cli_id = {Utils.Substituir};",
    //     $"UPDATE fornecedor SET Nome = 'Queiroz (Atualizado)' WHERE for_id = {Utils.Substituir};",
    //     $"UPDATE veiculo SET desc_veiculo = 'Caminhao mercedes (Atualizado)' WHERE id_veiculo = {Utils.Substituir};",
    //     $"UPDATE funcao SET desc_funcao = 'administrador (Atualizado)' WHERE id_funcao = {Utils.Substituir};",
    //     $"UPDATE Banco SET desc_banco = 'Bradesco (Atualizado)' WHERE id_banco = {Utils.Substituir};",
    //     $"UPDATE fluxo_caixa SET vl_pd = 4000 WHERE id = {Utils.Substituir};",
    //     $"UPDATE Fabricante SET nome_fabricante = 'Tigre ltda (Atualizado)' WHERE id_fabricante = {Utils.Substituir};",
    //     $"UPDATE Setor SET setor_produto = 'Hidraulica (Atualizado)' WHERE id_Setor = {Utils.Substituir};",
    //     $"UPDATE Localizacao_Prod SET Corredor = '1a (Atualizado)' WHERE id_loc = {Utils.Substituir};",
    //     $"UPDATE Forma_pag SET desc_formPag = 'Dinheiro (Atualizado)' WHERE id_formaPag = {Utils.Substituir};",
    //     $"UPDATE funcionario SET Nome = 'Lucas Oliveira (Atualizado)' WHERE id = {Utils.Substituir};",
    //     $"UPDATE usuario SET user_log = 'adm (Atualizado)' WHERE user_id = {Utils.Substituir};",
    //     $"UPDATE Produto SET descricao = 'Torneira (Atualizado)' WHERE id_produto = {Utils.Substituir};",
    //     $"UPDATE Item_venda SET prec_unitario = 100 WHERE id_Item = {Utils.Substituir};",
    //     $"UPDATE estoque SET dataADD = '2015-07-16 (Atualizado)' WHERE id_estoque = {Utils.Substituir};",
    //     $"UPDATE vendas SET entrega = '23/05/1998 (Atualizado)' WHERE id_venda = {Utils.Substituir};",
    //     $"UPDATE entrega SET rua = 'Rua (Atualizado)' WHERE id_entrega = {Utils.Substituir};"
    // };
    //
    // private static readonly IList<string> UpdateComChaveEstrangeira = new List<string>
    // {
    //     $"UPDATE cliente SET Nome = 'Lucas Oliveira (Atualizado)' WHERE cli_id = {Utils.Substituir};",
    //     $"UPDATE fornecedor SET Nome = 'Queiroz (Atualizado)' WHERE for_id = {Utils.Substituir};",
    //     $"UPDATE veiculo SET desc_veiculo = 'Caminhao mercedes (Atualizado)' WHERE id_veiculo = {Utils.Substituir};",
    //     $"UPDATE funcao SET desc_funcao = 'administrador (Atualizado)' WHERE id_funcao = {Utils.Substituir};",
    //     $"UPDATE Banco SET desc_banco = 'Bradesco (Atualizado)' WHERE id_banco = {Utils.Substituir};",
    //     $"UPDATE fluxo_caixa SET vl_pd = 4000 WHERE id = {Utils.Substituir};",
    //     $"UPDATE Fabricante SET nome_fabricante = 'Tigre ltda (Atualizado)' WHERE id_fabricante = {Utils.Substituir};",
    //     $"UPDATE Setor SET setor_produto = 'Hidraulica (Atualizado)' WHERE id_Setor = {Utils.Substituir};",
    //     $"UPDATE Localizacao_Prod SET Corredor = '1a (Atualizado)' WHERE id_loc = {Utils.Substituir};",
    //     $"UPDATE Forma_pag SET desc_formPag = 'Dinheiro (Atualizado)' WHERE id_formaPag = {Utils.Substituir};",
    //     $"UPDATE funcionario SET fun_id = {Utils.Substituir}, banco_id = {Utils.Substituir} WHERE id = {Utils.Substituir};",
    //     $"UPDATE usuario SET func_id = {Utils.Substituir} WHERE user_id = {Utils.Substituir};",
    //     $"UPDATE Produto SET id_Setor = {Utils.Substituir}, id_fabricante = {Utils.Substituir} WHERE id_produto = {Utils.Substituir};",
    //     $"UPDATE Item_venda SET id_produto = {Utils.Substituir} = 100 WHERE id_Item = {Utils.Substituir};",
    //     $"UPDATE estoque SET id_loc = {Utils.Substituir}, for_id = {Utils.Substituir}, id_produto = {Utils.Substituir} WHERE id_estoque = {Utils.Substituir};",
    //     $"UPDATE vendas SET fun_id = {Utils.Substituir}, id_itemVenda = {Utils.Substituir}, id_formaPag = {Utils.Substituir} WHERE id_venda = {Utils.Substituir};",
    //     $"UPDATE entrega SET id_venda = {Utils.Substituir}, id_veiculo = {Utils.Substituir} WHERE id_entrega = {Utils.Substituir};"
    // };

    #endregion
}

