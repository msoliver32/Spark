# Desafio Spark

### Ambiente
Windows 10
Python 3.7.2
JDK 1.8.0 v211
Spark 2.4.3

### Pre-requisitos
Configurar JDK, Spark, Hadoop e variáveis de ambiente. 
Tutorial de instalação Spark no Windows: [https://bigdata-madesimple.com/guide-to-install-spark-and-use-pyspark-from-jupyter-in-windows/]
Descompactar os logs da NASA que estão dentro da pasta files na raíz do projeto.
No terminal dentro da pasta do projeto executar:
```
pip install -r requirements.txt
```
### Executando:
Executar no terminal na pasta raíz do projeto: 
```
python spark.py
```
Os resultados são gravados na pasta results do projeto, cada arquivo .csv tem seu número indicando o exercício realizado.


# Respostas:

Qual o objetivo do comando cache em spark?
Objetivo é salvar em memória os RDDs para que possam ser utilizados posteriormente durante os estágios sem precisar recomputar novamente.

O mesmo código implementado em Spark é normalmente mais rápido que a implementação equivalente em MapReduce. Por que?
O MapReduce ao realizar o processamento durante os estágios persiste informações em disco e isto torna mais lento devido a latência, porém mais tolerante a falha. O Spark não persiste esses dados em memória de disco, ele mantem os dados In Memory, tornando o processamento muito mais rápido durante os estágios.

Qual é a função do SparkContext?
Sua função é realizar a conexão entre o Spark (DriveProgram) com o Cluster para utilizar o RDD em conjunto com os nós.

Explique com suas palavras o que é Resilient Distributed Datasets (RDD)
É uma coleção de dados que pode ser distribuída em parte ou total em nós ou clusters, ele permite a computação paralela.

GroupByKey é menos eficiente que reduceByKey em grandes dataset. Por quê?
reduceBykey combina os dados em cada partição e envia apenas uma vez para cada chave pela rede, faz-se necessário combinar todos valores em um mesmo tipo. groupByKey envia dados pela rede para o clusters sem somar o dados, deixando essa tarefa para os estágios de reduce, gerando grande tráfego de rede o causando latência no processamento.

Explique o que o código Scala abaixo faz.
Linha 1: É criado uma constante ‘textFile’ uma referencia ao RDD carregado de um arquivo de um sistema de arquivos distribuído (HDFS).
Linha 2: É criando a constante ‘counts’, é realizado a função flatMap que pode gerar para cada linha do RDD zero, 1 ou mais linhas no novo RDD gerando pelo estágio de transformação com base função de split separado por espaço “ “.
Linha 3: A partir do RDD gerado é feito um map transformado cada elemento (word) encontrado em uma tupla (word, 1) para ser realizada a contagem de palavras.
Linha 4: É utilizado a função reduceByKey e é também uma notação do Scala para somatória de todos os valores para a mesma chave, resultando assim uma coleção com uma tupla de valores, e cada elemento desta coleção possui uma palavra (texto) e a quantidade de vezes que ele aparece no arquivo original. O resultado é armazenado em ‘counts’.
Linha 5: O RDDs com a quantidade de vezes que cada palavra aparece no texto original é persistindo em arquivo.
