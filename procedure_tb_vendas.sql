CREATE OR REPLACE PROCEDURE `data-engineering-3254.trusted_vendas.sp_tb_vendas`()
BEGIN
  -- Declaração das variáveis
  DECLARE VAR_PROJETO STRING DEFAULT 'data-engineering-3254';
  DECLARE VAR_DATASET_RAW STRING DEFAULT 'raw_vendas';
  DECLARE VAR_DATASET_TRUSTED STRING DEFAULT 'trusted_vendas';
  DECLARE VAR_TABELA_DESTINO STRING DEFAULT 'tb_vendas';
  DECLARE VAR_TABELA_2017 STRING DEFAULT 'base_2017';
  DECLARE VAR_TABELA_2018 STRING DEFAULT 'base_2018';
  DECLARE VAR_TABELA_2019 STRING DEFAULT 'base_2019';

  -- Criação da tabela temporária temp_vendas
  EXECUTE IMMEDIATE  """
  CREATE OR REPLACE TEMP TABLE temp_vendas AS
  SELECT 
      ID_MARCA,
      MARCA,
      ID_LINHA,
      LINHA,
      DATA_VENDA,
      QTD_VENDA
  FROM 
      `""" || VAR_PROJETO || '.' || VAR_DATASET_RAW || '.' || VAR_TABELA_2017 || """`

  UNION ALL

  SELECT 
      ID_MARCA,
      MARCA,
      ID_LINHA,
      LINHA,
      DATA_VENDA,
      QTD_VENDA
  FROM 
      `""" || VAR_PROJETO || '.' || VAR_DATASET_RAW || '.' || VAR_TABELA_2018 || """`

  UNION ALL

  SELECT 
      ID_MARCA,
      MARCA,
      ID_LINHA,
      LINHA,
      DATA_VENDA,
      QTD_VENDA
  FROM 
      `""" || VAR_PROJETO || '.' || VAR_DATASET_RAW || '.' || VAR_TABELA_2019 || """`;
  """;

  -- Trunca a tabela de destino antes de inserir os novos dados
  EXECUTE IMMEDIATE  """ 
  TRUNCATE TABLE `""" || VAR_PROJETO || '.' || VAR_DATASET_TRUSTED || '.' || VAR_TABELA_DESTINO || """`;
  """;

  -- Insere os dados da tabela temporária para a tabela de destino
  EXECUTE IMMEDIATE  """
    INSERT INTO `""" || VAR_PROJETO || '.' || VAR_DATASET_TRUSTED || '.' || VAR_TABELA_DESTINO || """`
        (ID_MARCA, 
        MARCA, 
        ID_LINHA, 
        LINHA, 
        DATA_VENDA, 
        QTD_VENDA)
    SELECT 
        ID_MARCA, 
        MARCA, 
        ID_LINHA, 
        LINHA, 
        DATA_VENDA, 
        QTD_VENDA
    FROM temp_vendas
 """;

END;
