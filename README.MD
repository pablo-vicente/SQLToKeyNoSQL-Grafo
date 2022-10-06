# Rodando Programa

- Baixar [DOCKER](https://www.docker.com/)
- Rodar Arquivo ```docker-compose.yml```, instâncias dos bancos de dados utlizados ([Mongo](https://www.mongodb.com/) e [Neo4j](https://neo4j.com/))
- MONGO: acessível através do [localhost:8081](http://localhost:8081) (utilizado para armazenar dicionario)
  - User: ``root``
  - PWS: ``root``
- NEO4J(Enterprise): acessível através da [localhost:7474](http://localhost:7474)
    - User: ``neo4j``
    - PWS: ``pAsSw0rD``
- API SQLTONOKEYNOSQL: é possível utilizar o [SWAGER-Documentação](http://localhost:8080/swagger) ou [POSTAM](https://www.postman.com/) para os testes da API
- O arquivo [bd_matConstru.sql](.\Scripts\bd_matConstru.sql) contém alguns exemplos de SQL 
- O arquivo [Setup](.\Scripts\setup.js) contém exemplo de utilização da API com JS

## PASSOS

# 1 Criar Conexão com SGBD através da rota ```POST/no-sql-target```
> Dados de conexão Neo4j
```JSON
{
  "connector": "NEO4J",
  "name": "neo4j",
  "password": "pAsSw0rD",
  "url": "bolt://localhost:7687",
  "user": "neo4j"
}
```
> É possivel verificar criaçao através ```GET/no-sql-targets```

# 2 Definir o banco de dados através ```POST/current-database```
- Se o banco não existir será criado, se existir será apenas definido
```JSON
{
  "name": "bd_matConstru"
}
```
> É possivel verificar criaçao através ```GET/databases```
> È possível verificar se está definido através ```GET/current-database```

# 3 Rodar os Scripts através ```/query```
* Exemplo
```SQL
create table funcao(
    id_funcao int not null primary key auto_increment,
    desc_funcao varchar(50),
    salario double,
    carga_horaria varchar(30)
);
```
> Entrada API
```JSON
{
  "value": "create table funcao(id_funcao int not null primary key auto_increment,desc_funcao varchar(50),salario double,carga_horaria varchar(30));"
}
```
### 3.1 Rodar os Arquivo SQL através ```/query-file-sql-script```

# 4 Realizando o Transferência para Banco NoSQL
### 4.1 Script DDL (Criação tabelas) - A camada não faz a leitura automatica das definições das tabelas de um banco.
 - CREATE
 - ALTER
 - DROP
### 4.1 Script DML
- INSERT
- UPDATE
- SELECT
- DELETE

# 5 Sintaxe Suportada
O Parse SQL utilizado suporta um conjunto limitado de sintaxe, para mais detalhes [JSqlParser](https://github.com/JSQLParser/JSqlParser).
### Exemplo utilizados no Experimento:
### CREATE
```SQL
CREATE TABLE usuario(
-- TODOS os campos devem ser declarados, os tipos São ignorados
user_id int primary key auto_increment, -- Declaração Primary Key
func_id int not null,
    constraint fk_us_fun -- Declaração Foreign Key
    foreign key(func_id) -- Declaração Foreign Key, especificar ATRIBUTO
    references funcionario(id), -- Declaração Foreign Key, especificar TABELE E ATRIBUTO
user_log varchar(30),
user_pwd varchar(20)
);
```
### ALTER
```SQL
-- Não é permitido alterar CHAVE PRIMARIA
-- Pertimido Renomear Coluna
-- Pertimido Remover Coluna
-- Pertimido Adicionar Coluna
-- Suporta mais de uma Instrução
ALTER TABLE funcionario
  RENAME COLUMN razao_social TO nome_pessoa_juridica,
  DROP COLUMN nome_pessoa_juridica,
  ADD razao_social varchar(100);
```
### DROP
```SQL
-- Não é permitido deletar registros que são chaves estrangeiras de outros registros
 DROP TABLE funcionario
```

### INSERT
```SQL
-- Obrigatório Declarar todas as colunas, inclusive a Chave Primaria mesmo Sendo Id
-- Auto incremento ainda não é suportado
-- Suporta N Values
-- Não é permitido inserir registros com Chaves estrangeiras inexistentes.
INSERT INTO usuario (user_id, func_id, user_log, user_pwd)  -- 
VALUES 
    (1,1,'adm','adm'), 
    (2,1,'adm','adm')
```

### UPDATE
```SQL
-- Não é permitido atulizar registros com Chaves estrangeiras inexistentes.
UPDATE funcionario
SET razao_social = 'Razão Social', nome = "Homer Simpsons"
WHERE id = 1;
```

### SELECT
```SQL
SELECT * FROM funcionario;
SELECT id FROM funcionario WHERE nome = "Nome";
```

### DELETE
```SQL
-- Não é permitido deletar registros que são Chaves estrangeiras de outros registros.
DELETE FROM funcionario WHERE id >= "1";
```



















