![img.png](src/main/resources/static/images/logo.png)
# SqlToNoSQL
## NoSQL
```text
Neo4j => OK | Funcional
Mongo => Parcial (Requer otimização)
Cassandra => Parcial (Requer otimização) | Não testado
Redis => Parcial (Requer otimização) | Não testado
SimpleDB => Parcial (Requer otimização) | Não testado
Voldemor => Parcial (Requer otimização) | Não testado
```

## Rodando Programa

- Baixar [DOCKER](https://www.docker.com/)
- Rodar Arquivo ```docker-compose.yml```, instâncias dos bancos de dados utlizados ([Mongo](https://www.mongodb.com/) e [Neo4j](https://neo4j.com/))
```
docker-compose up docker-compose.yml
```
- MONGO: acessível através do [localhost:8081](http://localhost:8081) (utilizado para armazenar dicionario)
  - User: ``root``
  - PWS: ``root``
- NEO4J(Enterprise): acessível através da [localhost:7474](http://localhost:7474)
    - User: ``neo4j``
    - PWS: ``pAsSw0rD``
- API SQLTONOKEYNOSQL: é possível utilizar o [SWAGGER](http://localhost:8080/swagger), [POSTAM](https://www.postman.com/) ou a [Interface Web](http://localhost:8080/).
- O arquivo [bd_matConstru.sql](.\Scripts\bd_matConstru.sql) contém alguns exemplos de SQL 
- O arquivo [Setup](.\Scripts\setup.js) contém exemplo de utilização da API com JS

## PASSOS

# 1 Criar Conexão com SGBD através da rota ```POST/no-sql-target```
> Através do ```GET/connectors``` é possível obter os SGBDs suportados
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

# 2 Criar e definir o banco de dados através ```POST/database```
```JSON
{
  "name": "bd_matConstru",
  "connector": "NEO4J"
}
```
> É possivel verificar criaçao através ```GET/databases```
 
> É possível verificar se está definido através ```GET/current-database```

## 2.1 Alterar o banco de execução através ```POST/current-database```
```JSON
{
  "name": "bd_matConstru",
  "connector": "NEO4J"
}
```

# 3 Rodar os Scripts através ```/query``` 
> É possível executar arquivos SQL através ```/query-file-sql-script```
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

# 4 Realizando o Transferência para Banco NoSQL
> A camada não faz a leitura automatica das definições das tabelas de um banco.
## 4.1 Script DDL 
 - CREATE
 - ALTER
 - DROP
## 4.1 Script DML
- INSERT
- UPDATE
- SELECT
- DELETE

# 5 Sintaxe Suportada
> O Parse SQL utilizado suporta um conjunto limitado de sintaxe, para mais detalhes [JSqlParser](https://github.com/JSQLParser/JSqlParser).
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

# Interface WEB
![img.png](img_1.png)
![img.png](img.png)
![img_2.png](img_2.png)
![img_3.png](img_3.png)



















