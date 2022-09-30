ALTER TABLE cliente
    RENAME COLUMN razao_social TO razao_social_renomeada,
    RENAME COLUMN cidade TO cidade_renomeada;
ALTER TABLE fluxo_caixa
    RENAME COLUMN contAreceber TO contAreceber_renomeada,
    RENAME COLUMN contApagar TO contApagar_renomeada;
ALTER TABLE estoque
    RENAME COLUMN id_loc TO id_loc_renomeada,
    RENAME COLUMN for_id TO for_id_renomeada;
ALTER TABLE Localizacao_Prod
    RENAME COLUMN Corredor TO Corredor_renomeada,
    RENAME COLUMN partilhera TO partilhera_renomeada;
ALTER TABLE fornecedor
    RENAME COLUMN Nome TO Nome_renomeada,
    RENAME COLUMN telefone TO telefone_renomeada;
ALTER TABLE entrega
    RENAME COLUMN id_venda TO id_venda_renomeada,
    RENAME COLUMN id_veiculo TO id_veiculo_renomeada;
ALTER TABLE veiculo
    RENAME COLUMN placa_veiculo TO placa_veiculo_renomeada,
    RENAME COLUMN ano_veiculo TO ano_veiculo_renomeada;
ALTER TABLE vendas
    RENAME COLUMN id_itemVenda TO id_itemVenda_renomeada,
    RENAME COLUMN id_formaPag TO id_formaPag_renomeada;
ALTER TABLE Item_venda
    RENAME COLUMN id_produto TO id_produto_renomeada,
    RENAME COLUMN quant_vendida TO quant_vendida_renomeada;
ALTER TABLE Produto
    RENAME COLUMN id_Setor TO id_Setor_renomeada,
    RENAME COLUMN id_fabricante TO id_fabricante_renomeada;
ALTER TABLE Setor
    RENAME COLUMN setor_produto TO setor_produto_renomeada,
    RENAME COLUMN sub_setor_produto TO sub_setor_produto_renomeada;
ALTER TABLE Fabricante
    RENAME COLUMN nome_fabricante TO nome_fabricante_renomeada,
    RENAME COLUMN marca_fabricante TO marca_fabricante_renomeada;
ALTER TABLE Forma_pag
    RENAME COLUMN desc_formPag TO desc_formPag_renomeada;
ALTER TABLE usuario
    RENAME COLUMN func_id TO func_id_renomeada,
    RENAME COLUMN user_log TO user_log_renomeada;
ALTER TABLE funcionario
    RENAME COLUMN funcao_id TO funcao_id_renomeada,
    RENAME COLUMN banco_id TO banco_id_renomeada;
ALTER TABLE funcao
    RENAME COLUMN desc_funcao TO desc_funcao_renomeada,
    RENAME COLUMN salario TO salario_renomeada;
ALTER TABLE Banco
    RENAME COLUMN desc_banco TO desc_banco_renomeada;
