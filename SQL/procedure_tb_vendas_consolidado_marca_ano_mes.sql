CREATE OR REPLACE PROCEDURE `data-engineering-3254.trusted_vendas.sp_tb_vendas_consolidado_marca_ano_mes`()
BEGIN
  -- Declaração das variáveis
  DECLARE VAR_PROJETO STRING DEFAULT 'data-engineering-3254';
  DECLARE VAR_DATASET_TRUSTED STRING DEFAULT 'trusted_vendas';
  DECLARE VAR_TABELA_DESTINO STRING DEFAULT 'tb_vendas_consolidado_marca_ano_mes';
  DECLARE VAR_TABELA_ORIGEM STRING DEFAULT 'tb_vendas';

  -- Criação da tabela temporária temp_consolidado_marca_ano_mes
  EXECUTE IMMEDIATE """ 
  CREATE OR REPLACE TEMP TABLE temp_consolidado_marca_ano_mes AS
  SELECT
      MARCA,
      EXTRACT(YEAR FROM DATA_VENDA) AS ANO,
      EXTRACT(MONTH FROM DATA_VENDA) AS MES,
      SUM(QTD_VENDA) AS TOTAL_VENDAS
  FROM
    `""" || VAR_PROJETO || '.' || VAR_DATASET_TRUSTED || '.' || VAR_TABELA_ORIGEM || """`
  GROUP BY 
      MARCA, 
      ANO, 
      MES;
""";

  -- Trunca a tabela de destino antes de inserir os novos dados
  EXECUTE IMMEDIATE  """ 
  TRUNCATE TABLE `""" || VAR_PROJETO || '.' || VAR_DATASET_TRUSTED || '.' || VAR_TABELA_DESTINO || """`;
  """;

  -- Insere os dados da tabela temporária para a tabela de destino
  EXECUTE IMMEDIATE """
    INSERT INTO `""" || VAR_PROJETO || '.' || VAR_DATASET_TRUSTED || '.' || VAR_TABELA_DESTINO || """` 
      (MARCA, 
      ANO, 
      MES, 
      TOTAL_VENDAS)
    SELECT 
      MARCA, 
      ANO, 
      MES, 
      TOTAL_VENDAS
    FROM temp_consolidado_marca_ano_mes
""";
END;
