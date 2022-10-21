create table funcao(
id_funcao int not null primary key auto_increment,
desc_funcao varchar(50),
salario double,
carga_horaria varchar(30)
);

create table Banco(
id_banco int primary key auto_increment,
desc_banco varchar(30)
);

create table funcionario(
id int not null primary key auto_increment,
funcao_id int not null,
    constraint fk_func_fun 
    foreign key(funcao_id) references funcao(id_funcao),
banco_id int,
    constraint fk_func_ban
    foreign key(banco_id) references Banco(id_banco),
Nome varchar(70),
data_nasc date,
telefone varchar(30),
email varchar(50),
t_pessoa varchar(30),
razao_social varchar(30),
RG varchar(40),
CPf varchar(60),
endereco varchar(60),
cep varchar(15),
cnh varchar(50),
cat varchar(60),
ctps varchar(50),
bairro varchar(30),
cidade varchar(40),
uf varchar(4),
agencia varchar(30),
conta varchar(30)
);

create table usuario(
user_id int primary key auto_increment,
func_id int not null,
    constraint fk_us_fun 
    foreign key(func_id)
    references funcionario(id),
user_log varchar(30),
user_pwd varchar(20)
);

create table fluxo_caixa(
id int primary key auto_increment,
vl_pd double,
vl_nd double,
vl_pm double,
vl_nm double,
contAreceber double,
contApagar double
);

create table cliente(
cli_id int not null primary key auto_increment,
Nome varchar(70),
data_nasc date,
telefone varchar(30),
email varchar(50),
RG varchar(40),
CPf varchar(60),
endereco varchar(60),
razao_social varchar(50),
t_Pessoa varchar (20),
cep varchar (30),
cidade varchar(20),
bairro varchar(30),
uf varchar(20)
);

create table fornecedor(
for_id int not null primary key auto_increment,
Nome varchar(70),
data_nasc varchar (20),
telefone varchar(30),
email varchar(50),
site varchar(50),
RG varchar(40),
CPf varchar(60),
endereco varchar(60),
razao_social varchar(50),
t_Pessoa varchar (20),
cnpj varchar (20),
cep varchar (30),
ins_Est varchar(30),
cidade varchar(20),
bairro varchar(30),
uf varchar(20)
);

create table Fabricante(
id_fabricante int primary key auto_increment,
nome_fabricante varchar(25),
marca_fabricante varchar(25)
);

create table Setor(
id_Setor int primary key auto_increment,
setor_produto varchar(30),
sub_setor_produto varchar(20),
tipo varchar(20)
);

create table Produto(
id_produto int primary key auto_increment,
id_Setor int not null,
    constraint fk_pro_set 
    foreign key(id_Setor) references setor(id_Setor),
id_fabricante int not null,
    constraint fk_pro_fab 
    foreign key(id_fabricante) references fabricante(id_fabricante),
cod_barra double,
descricao varchar(50),
marca varchar(30),
unidade_med varchar(10),
preco_compra double,
preco_venda double,
margem_lucro double
);

create table Localizacao_Prod(
id_loc int primary key auto_increment,
Corredor varchar(10),
partilhera varchar(10),
gaveta varchar(10)
);

create table estoque(
id_estoque int not null primary key auto_increment,
id_loc int not null,
    constraint fk_est_loc
    foreign key(id_loc) references localizacao_Prod(id_loc),
for_id int not null,
    constraint fk_est_forn 
    foreign key(for_id) references fornecedor(for_id),
id_produto int not null,
    constraint fk_est_prod
    foreign key(id_produto) references produto(id_produto),
quant_disponivel int,
dataADD datetime
);

create table Item_venda(
id_Item int primary key ,
id_produto int,
    constraint fk_item_Prod 
    foreign key(id_produto) references produto(id_produto),
quant_vendida int,
prec_unitario double
);

create table Forma_pag(
id_formaPag int primary key auto_increment,
desc_formPag varchar(20)
);

create table vendas(
id_venda int primary key auto_increment,
fun_id int,
    constraint fk_vend_func 
    foreign key(fun_id) references funcionario(id),
id_itemVenda int,
    constraint fk_vend_item 
    foreign key(id_itemVenda) references Item_venda(id_item),
id_formaPag int,
    constraint fk_vend_formaP
    foreign key(id_formaPag) references forma_pag(id_formaPag),
valor_venda double,
valor_recebido double,
troco double,
data_venda datetime,
entrega varchar(1)
);

create table veiculo(
id_veiculo int primary key auto_increment,
desc_veiculo varchar(40),
ano_veiculo varchar(12),
placa_veiculo varchar(17)
);

create table entrega(
id_entrega int primary key auto_increment,
id_venda int,
    constraint fk_ent_Vend 
    foreign key(id_venda) references vendas(id_venda),
id_veiculo int,
    constraint fk_ent_Veic
    foreign key(id_veiculo) references veiculo(id_veiculo),
rua varchar(70),
numero varchar(7),
bairro varchar(20),
cidade varchar(30),
uf varchar(4),
cep varchar(20)
);
