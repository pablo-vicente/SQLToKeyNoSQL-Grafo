ALTER TABLE cliente
    RENAME COLUMN razao_social_renomeada TO razao_social,
    RENAME COLUMN cidade_renomeada TO cidade;
ALTER TABLE fluxo_caixa
    RENAME COLUMN contAreceber_renomeada TO contAreceber,
    RENAME COLUMN contApagar_renomeada TO contApagar;
ALTER TABLE estoque
    RENAME COLUMN id_loc_renomeada TO id_loc,
    RENAME COLUMN for_id_renomeada TO for_id;
ALTER TABLE Localizacao_Prod
    RENAME COLUMN Corredor_renomeada TO Corredor,
    RENAME COLUMN partilhera_renomeada TO partilhera;
ALTER TABLE fornecedor
    RENAME COLUMN Nome_renomeada TO Nome,
    RENAME COLUMN telefone_renomeada TO telefone;
ALTER TABLE entrega
    RENAME COLUMN id_venda_renomeada TO id_venda,
    RENAME COLUMN id_veiculo_renomeada TO id_veiculo;
ALTER TABLE veiculo
    RENAME COLUMN placa_veiculo_renomeada TO placa_veiculo,
    RENAME COLUMN ano_veiculo_renomeada TO ano_veiculo;
ALTER TABLE vendas
    RENAME COLUMN id_itemVenda_renomeada TO id_itemVenda,
    RENAME COLUMN id_formaPag_renomeada TO id_formaPag;
ALTER TABLE Item_venda
    RENAME COLUMN id_produto_renomeada TO id_produto,
    RENAME COLUMN quant_vendida_renomeada TO quant_vendida;
ALTER TABLE Produto
    RENAME COLUMN id_Setor_renomeada TO id_Setor,
    RENAME COLUMN id_fabricante_renomeada TO id_fabricante;
ALTER TABLE Setor
    RENAME COLUMN setor_produto_renomeada TO setor_produto,
    RENAME COLUMN sub_setor_produto_renomeada TO sub_setor_produto;
ALTER TABLE Fabricante
    RENAME COLUMN nome_fabricante_renomeada TO nome_fabricante,
    RENAME COLUMN marca_fabricante_renomeada TO marca_fabricante;
ALTER TABLE Forma_pag
    RENAME COLUMN desc_formPag_renomeada TO desc_formPag;
ALTER TABLE usuario
    RENAME COLUMN func_id_renomeada TO func_id,
    RENAME COLUMN user_log_renomeada TO user_log;
ALTER TABLE funcionario
    RENAME COLUMN funcao_id_renomeada TO funcao_id,
    RENAME COLUMN banco_id_renomeada TO banco_id;
ALTER TABLE funcao
    RENAME COLUMN desc_funcao_renomeada TO desc_funcao,
    RENAME COLUMN salario_renomeada TO salario;
ALTER TABLE Banco
    RENAME COLUMN desc_banco_renomeada TO desc_banco;
