create table funcao(
id_funcao int not null primary key auto_increment,
desc_funcao varchar(50),
salario double,
carga_horaria varchar(30)
);

insert into funcao (id_funcao, desc_funcao,salario,carga_horaria)
values(1,'administrador',2000.00,'40 horas semanais');

create table Banco(
id_banco int primary key auto_increment,
desc_banco varchar(30)
);

insert into Banco (id_banco, desc_banco) values (1,'Bradesco');

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

insert into funcionario (id, funcao_id,banco_id,Nome,data_nasc,telefone,email,t_pessoa,razao_social,RG,CPf,endereco,cep,cnh,cat,ctps,bairro,cidade,uf,agencia,conta)
values(1,1,1,'Lucas Oliveira','1998-04-23','(19)3333-3333','Lucas@com.net','Pessoa Física','num sei cara','45.343.942-1','450.802.308.86','Rua: vinicius de Moraes n°846','13.188-103','1111','2222','3333','Jd.Amanda','Hortolândia','sp','4444','5555');

create table usuario(
user_id int primary key auto_increment,
func_id int not null,
    constraint fk_us_fun 
    foreign key(func_id) references funcionario(id),
user_log varchar(30),
user_pwd varchar(20)
);

insert into usuario (user_id, func_id, user_log, user_pwd) 
values (1,1,'adm','adm');

create table fluxo_caixa(
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

insert into cliente(cli_id,Nome,data_nasc,telefone,email,RG,CPf,endereco,razao_social,t_Pessoa,cep,cidade,bairro,uf) values 
(1,'Lucas Oliveira','1998-04-23','(19)3333-3333','Lucas@com.net','45.343.942-1','450.802.308.86','Rua: vinicius de Moraes n°846','1111','pessoa fisica','13.188-103','Hortolândia','Jd.Amanda','sp');

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

insert into fornecedor (for_id,Nome,data_nasc,telefone,email,site,RG,CPf,endereco,razao_social,t_Pessoa,cnpj,cep,ins_Est,cidade,bairro,uf)
values (1,'Queiroz','2000-02-02','(19)9999-6666','kkj','jkjk','45.444.444-4','444.444.444-44','j','ftfy','Pesso juridica','12','12.111-111','kjk','hi','jd','SP');

create table Fabricante(
id_fabricante int primary key auto_increment,
nome_fabricante varchar(25),
marca_fabricante varchar(25)
);

insert into Fabricante(id_fabricante, nome_fabricante,marca_fabricante)
values (1,'Tigre ltda','tigre');

create table Setor(
id_Setor int primary key auto_increment,
setor_produto varchar(30),
sub_setor_produto varchar(20),
tipo varchar(20)
);

insert into Setor(id_Setor, setor_produto,sub_setor_produto,tipo)
values (1, 'Hidraulica','valvula','Premium');

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

insert into Produto (id_produto, id_setor,id_fabricante,cod_barra,descricao,marca,unidade_med,preco_compra,preco_venda,margem_lucro)
values(1,1,1,12345678,'Torneira','tigre','unid',10,20,100);

create table Localizacao_Prod(
id_loc int primary key auto_increment,
Corredor varchar(10),
partilhera varchar(10),
gaveta varchar(10)
);

insert into Localizacao_Prod(id_loc,Corredor,partilhera,gaveta)
values(1,'1a','1a','1a');

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

insert into estoque (id_estoque,id_loc,for_id,id_produto,quant_disponivel,dataADD) 
values (1,1,1,1,100,'2015-07-16');

create table Item_venda(
id_Item int primary key ,
id_produto int,
    constraint fk_item_Prod 
    foreign key(id_produto) references produto(id_produto),
quant_vendida int,
prec_unitario double
);

insert into Item_venda (id_Item,id_produto,quant_vendida, prec_unitario) 
values (1,1,1,100);

create table Forma_pag(
id_formaPag int primary key auto_increment,
desc_formPag varchar(20)
);

insert into Forma_pag (id_formaPag,desc_formPag)
values (1,'Dinheiro'); 

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
insert into vendas (id_venda,fun_id,id_itemVenda,id_formaPag,valor_venda, valor_recebido, troco, data_venda, entrega)
values (1,1,1,1,1000, 2000, 1000, '2004-05-23T14:25:10', '23/05/1998')

create table veiculo(
id_veiculo int primary key auto_increment,
desc_veiculo varchar(40),
ano_veiculo varchar(12),
placa_veiculo varchar(17)
);

insert into veiculo (id_veiculo,desc_veiculo,ano_veiculo,placa_veiculo)
values (1,'Caminhao mercedes','2017','abc-6666');


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

insert into entrega (id_entrega, id_venda, id_veiculo, rua, numero, bairro, cidade, uf, cep)
values (1, 1, 1, "Rua", "Numero", "bairro", "cidade", "uf", "cep")
