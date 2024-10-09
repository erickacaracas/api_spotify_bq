CREATE OR REPLACE PROCEDURE `data-engineering-3254.trusted_vendas.sp_tb_vendas_consolidado_marca_linha`()
BEGIN
  -- Declaração das variáveis
  DECLARE VAR_PROJETO STRING DEFAULT 'data-engineering-3254';
  DECLARE VAR_DATASET_TRUSTED STRING DEFAULT 'trusted_vendas';
  DECLARE VAR_TABELA_DESTINO STRING DEFAULT 'tb_vendas_consolidado_marca_linha';
  DECLARE VAR_TABELA_ORIGEM STRING DEFAULT 'tb_vendas';

  -- Criação da tabela temporária temp_consolidado_marca_linha
  EXECUTE IMMEDIATE """
  CREATE OR REPLACE TEMP TABLE temp_consolidado_marca_linha AS
    SELECT
      MARCA,
      LINHA,
      SUM(QTD_VENDA) AS TOTAL_VENDAS
    FROM
      `""" || VAR_PROJETO || '.' || VAR_DATASET_TRUSTED || '.' || VAR_TABELA_ORIGEM || """`
    GROUP BY 
        MARCA, 
        LINHA;
  """;

  -- Trunca a tabela de destino antes de inserir os novos dados
  EXECUTE IMMEDIATE """
  TRUNCATE TABLE `""" || VAR_PROJETO || '.' || VAR_DATASET_TRUSTED || '.' || VAR_TABELA_DESTINO || """`;
  """;

  -- Insere os dados da tabela temporária para a tabela de destino
  EXECUTE IMMEDIATE """
    INSERT INTO `""" || VAR_PROJETO || '.' || VAR_DATASET_TRUSTED || '.' || VAR_TABELA_DESTINO || """` 
      (MARCA, 
      LINHA, 
      TOTAL_VENDAS)
      SELECT 
        MARCA, 
        LINHA, 
        TOTAL_VENDAS
      FROM temp_consolidado_marca_linha
  """;
END;
