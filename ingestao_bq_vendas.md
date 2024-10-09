1. Realizar a importação dos dados dos 3 arquivos em uma tabela criada por você no
banco de dados de sua escolha.
 
   1.1  Upload dos arquivos no Cloud Storage. Foi necessário o tratamento para salvar o arquivo no formato .csv e conferir se está com codificação UTF-8
   
![image](https://github.com/user-attachments/assets/6db28191-1bb9-4710-b413-8ad9c6858cc8)

  1.2 Criação de tabelas no BigQuery com dados brutos. Para criação de tabelas utilizei a opção 'CREATE TABLE' > 'SOURCE Google Cloud Storage' > Schema - Auto Detected por verificar que os campos já estavam em formatos bem estruturados porém o ideal é criar todos os campos do tipo String na camda raw e depois realizar as transformações e tratamentos necessários. 
  Armazenei no dataset raw_vendas com mesmo nome do arquivo original.

  ![image](https://github.com/user-attachments/assets/d3c74209-e943-42d9-a3da-a6b3e94290cb)

3. Com os dados importados, modelar 4 novas tabelas e implementar processos que façam as transformações necessárias e insiram as seguintes visões nas tabelas:
   
    3.1 Tabela geral - tb_vendas: Arquivos DDL e procedure referentes â tb_vendas
   
      a. Tabela 1 - Consolidado de vendas por ano e mês: Arquivos DDL e procedure referentes â tb_vendas_consolidado_ano_mes
   
      b. Tabela 2 - Consolidado de vendas por marca e linha: Arquivos DDL e procedure referentes â tb_vendas_consolidado_marca_linha
   
      c. Tabela 3 - Consolidado de vendas por marca, ano e mês: Arquivos DDL e procedure referentes â tb_vendas_consolidado_marca_ano_mes
   
      d. Tabela 4 - Consolidado de vendas por linha, ano e mês: Arquivos DDL e procedure referentes â tb_vendas_consolidado_linha_ano_mes
