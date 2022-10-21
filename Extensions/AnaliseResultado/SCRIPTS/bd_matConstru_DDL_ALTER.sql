ALTER TABLE cliente
    RENAME COLUMN nome TO nome_renomeada,
    RENAME COLUMN data_nasc TO data_nasc_renomeada,
    RENAME COLUMN telefone TO telefone_renomeada,
    RENAME COLUMN email TO email_renomeada,
    RENAME COLUMN RG TO RG_renomeada,
    RENAME COLUMN CPf TO CPf_renomeada,
    RENAME COLUMN endereco TO endereco_renomeada,
    RENAME COLUMN razao_social TO razao_social_renomeada,
    RENAME COLUMN t_Pessoa TO t_Pessoa_renomeada,
    RENAME COLUMN cep TO cep_renomeada,
    RENAME COLUMN cidade TO cidade_renomeada,
    RENAME COLUMN bairro TO bairro_renomeada,
    RENAME COLUMN uf TO uf_renomeada;
ALTER TABLE fluxo_caixa
    RENAME COLUMN vl_pd TO vl_pd_renomeada,
    RENAME COLUMN vl_nd TO vl_nd_renomeada,
    RENAME COLUMN vl_pm TO vl_pm_renomeada,
    RENAME COLUMN vl_nm TO vl_pm_renomeada,
    RENAME COLUMN contAreceber TO contAreceber_renomeada,
    RENAME COLUMN contApagar TO contApagar_renomeada;
ALTER TABLE estoque
    RENAME COLUMN id_loc TO id_loc_renomeada,
    RENAME COLUMN for_id TO for_id_renomeada,
    RENAME COLUMN id_produto TO id_produto_renomeada,
    RENAME COLUMN quant_disponivel TO quant_disponivel_renomeada,
    RENAME COLUMN dataADD TO dataADD_renomeada;
ALTER TABLE Localizacao_Prod
    RENAME COLUMN Corredor TO Corredor_renomeada,
    RENAME COLUMN partilhera TO partilhera_renomeada,
    RENAME COLUMN gaveta TO gaveta_renomeada;
ALTER TABLE fornecedor
    RENAME COLUMN Nome TO Nome_renomeada,
    RENAME COLUMN data_nasc TO data_nasc_renomeada,
    RENAME COLUMN telefone TO telefone_renomeada,
    RENAME COLUMN email TO email_renomeada,
    RENAME COLUMN site TO site_renomeada,
    RENAME COLUMN RG TO RG_renomeada,
    RENAME COLUMN CPf TO CPf_renomeada,
    RENAME COLUMN endereco TO endereco_renomeada,
    RENAME COLUMN razao_social TO razao_social_renomeada,
    RENAME COLUMN t_Pessoa TO t_Pessoa_renomeada,
    RENAME COLUMN cnpj TO cnpj_renomeada,
    RENAME COLUMN cep TO cep_renomeada,
    RENAME COLUMN ins_Est TO ins_Est_renomeada,
    RENAME COLUMN cidade TO cidade_renomeada,
    RENAME COLUMN bairro TO bairro_renomeada,
    RENAME COLUMN uf TO uf_renomeada;
ALTER TABLE entrega
    RENAME COLUMN id_venda TO id_venda_renomeada,
    RENAME COLUMN id_veiculo TO id_veiculo_renomeada,
    RENAME COLUMN rua TO rua_renomeada,
    RENAME COLUMN numero TO numero_renomeada,
    RENAME COLUMN bairro TO bairro_renomeada,
    RENAME COLUMN cidade TO cidade_renomeada,
    RENAME COLUMN uf TO uf_renomeada,
    RENAME COLUMN cep TO cep_renomeada;
ALTER TABLE veiculo
    RENAME COLUMN desc_veiculo TO desc_veiculo_renomeada,
    RENAME COLUMN ano_veiculo TO ano_veiculo_renomeada,
    RENAME COLUMN placa_veiculo TO placa_veiculo_renomeada;
ALTER TABLE vendas
    RENAME COLUMN fun_id TO fun_id_renomeada,
    RENAME COLUMN id_itemVenda TO id_itemVenda_renomeada,
    RENAME COLUMN id_formaPag TO id_formaPag_renomeada,
    RENAME COLUMN valor_venda TO valor_venda_renomeada,
    RENAME COLUMN valor_recebido TO valor_recebido_renomeada,
    RENAME COLUMN troco TO troco_renomeada,
    RENAME COLUMN data_venda TO data_venda_renomeada,
    RENAME COLUMN entrega TO entrega_renomeada;
ALTER TABLE Item_venda
    RENAME COLUMN id_produto TO id_produto_renomeada,
    RENAME COLUMN quant_vendida TO quant_vendida_renomeada,
    RENAME COLUMN prec_unitario TO prec_unitario_renomeada;
ALTER TABLE Produto
    RENAME COLUMN id_Setor TO id_Setor_renomeada,
    RENAME COLUMN id_fabricante TO id_fabricante_renomeada,
    RENAME COLUMN cod_barra TO cod_barra_renomeada,
    RENAME COLUMN descricao TO descricao_renomeada,
    RENAME COLUMN marca TO marca_renomeada,
    RENAME COLUMN unidade_med TO unidade_med_renomeada,
    RENAME COLUMN preco_compra TO preco_compra_renomeada,
    RENAME COLUMN preco_venda TO preco_venda_renomeada,
    RENAME COLUMN margem_lucro TO margem_lucro_renomeada;
ALTER TABLE Setor
    RENAME COLUMN setor_produto TO setor_produto_renomeada,
    RENAME COLUMN sub_setor_produto TO sub_setor_produto_renomeada,
    RENAME COLUMN tipo TO tipo_renomeada;
ALTER TABLE Fabricante
    RENAME COLUMN nome_fabricante TO nome_fabricante_renomeada,
    RENAME COLUMN marca_fabricante TO marca_fabricante_renomeada;
ALTER TABLE Forma_pag
    RENAME COLUMN desc_formPag TO desc_formPag_renomeada;
ALTER TABLE usuario
    RENAME COLUMN func_id TO func_id_renomeada,
    RENAME COLUMN user_log TO user_log_renomeada,
    RENAME COLUMN user_pwd TO user_pwd_renomeada;
ALTER TABLE funcionario
    RENAME COLUMN funcao_id TO funcao_id_renomeada,
    RENAME COLUMN banco_id TO banco_id_renomeada,
    RENAME COLUMN Nome TO Nome_renomeada,
    RENAME COLUMN data_nasc TO data_nasc_renomeada,
    RENAME COLUMN telefone TO telefone_renomeada,
    RENAME COLUMN email TO email_renomeada,
    RENAME COLUMN t_pessoa TO t_pessoa_renomeada,
    RENAME COLUMN razao_social TO razao_social_renomeada,
    RENAME COLUMN RG TO RG_renomeada,
    RENAME COLUMN CPf TO CPf_renomeada,
    RENAME COLUMN endereco TO endereco_renomeada,
    RENAME COLUMN cep TO cep_renomeada,
    RENAME COLUMN cnh TO cnh_renomeada,
    RENAME COLUMN cat TO cat_renomeada,
    RENAME COLUMN ctps TO ctps_renomeada,
    RENAME COLUMN bairro TO bairro_renomeada,
    RENAME COLUMN cidade TO cidade_renomeada,
    RENAME COLUMN uf TO uf_renomeada,
    RENAME COLUMN agencia TO agencia_renomeada,
    RENAME COLUMN conta TO conta_renomeada;
ALTER TABLE funcao
    RENAME COLUMN desc_funcao TO desc_funcao_renomeada,
    RENAME COLUMN salario TO salario_renomeada,
    RENAME COLUMN carga_horaria TO carga_horaria_renomeada;
ALTER TABLE Banco
    RENAME COLUMN desc_banco TO desc_banco_renomeada;
