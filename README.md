Organização das atividades

![trello-board-qr-code](https://github.com/user-attachments/assets/85436430-b9bd-4ed9-8b2f-50c171752c0e)

Trello: https://trello.com/b/E6gW1Jf8 

# api_spotify_bq
### Case 2 - Etapa de Integração com API Spotify

Documentação de apoio e links úteis: 

https://developer.spotify.com/documentation/web-api 

https://github.com/psf/requests

https://requests.readthedocs.io/en/latest/

https://cloud.google.com/colab/docs/create-console-quickstart

https://cloud.google.com/colab/pricing?hl=pt-br

https://cloud.google.com/billing/docs/how-to/budgets?hl=pt-br

https://github.com/psf/requests 



### Passo a passo para Conexão API do Spotify com GCP:

**_1. Criação do Dataset para armazenamento das tabelas_**:


![image](https://github.com/user-attachments/assets/8e710469-f056-4feb-a607-d3909bc8256a)

**_2. Criação de uma service account e download da chave gerada_**:

  **[2.1]**  Para execução da extração de dados da API do Spotify e ingestão no BigQuery é necessária a utilização de uma service account com permissões adequadas.
   
- Passo a passo para criação da service account:
    	
   **a.** No Google Cloud Platform localizar nos serviços a opção ‘IAM & Admin’ > Service Accounts.

	 
![image](https://github.com/user-attachments/assets/b072fa81-0f2e-4ef7-bfff-3ed63852f49b) 



**b.** Crie uma service account e atribua permissões como  BigQuery Data Editor (ou BigQuery Admin para mais permissões), Storage Admin e Storage Object Viewer.

![image](https://github.com/user-attachments/assets/73540af3-9548-4dfa-b2ec-3d412eac1009)
![image](https://github.com/user-attachments/assets/ddfe5b6d-c0ae-4d77-93ea-2d2c5ba5bd48)


**[2.2]** Selecione a service account criada e na opção ‘KEYS’ crie uma nova chave e realize o download do arquivo JSON. Para minha execução nomeei o arquivo como chave_sa.json.

  
![image](https://github.com/user-attachments/assets/e2cadc0b-7ef9-4548-8e7a-5fecaace26ad)


**_3. Configuração do bucket no Google Cloud Storage_**

**[3.1]**  Bucket criado para armazenamento do arquivo de chave de service account gerado no passo 2.


![image](https://github.com/user-attachments/assets/4d39ed4b-f465-42f0-8e7b-4860ddccb7a8)


**[3.2]** Upload do arquivo chave_sa.json para o bucket


![image](https://github.com/user-attachments/assets/43e2dccf-837d-4b25-bc27-3f5ea6242730)


**_4. Configuração das Credenciais da API do Spotify no Secret Manager._**

**[4.1]** Conta criada no o Spotify Developer Dashboard

**a.** Aplicativo criado e acesso no https://developer.spotify.com/dashboard/ea4e9cace9284eca946515cac09ce737/settings 

**b.** Credenciais client_id e cliente_secret obtidas e armazenadas em um arquivo .json no seguinte formato. Para minha execução utilizei o nome spotify-credentials.

  
```
{
"client_id": "SEU_CLIENT_ID",
"client_secret": "SEU_CLIENT_SECRET"
}
```


**[4.2]** No Secret Manager do GCP selecionar opção ‘Create Secret' e ‘Upload File’ onde selecionei o arquivo .json spotify-credentials. Também realizei o upload da minha chave de service account porém para conexão da service account utilizarei na execução consultando o arquivo via Cloud Storage.


![image](https://github.com/user-attachments/assets/247107de-8481-4417-93c4-f77cb88b4555)  
	
![image](https://github.com/user-attachments/assets/3d12d65f-ac8c-4c81-939d-fcc7344411cf)

*Obs: Também é possível versionar os arquivos criados caso seja necessária uma atualização das credenciais.*

**_5. Considerações finais_**

Com a criação do dataset para armazenamento, service account com chave e as permissões necessárias concedidas e credenciamento na API do Spotify realizados com sucesso podemos seguir para o desenvolvimento do código Python que irá executar a extração de dados. Para melhor visualização abrir o arquivo spotify_podcasts.ipynb  com os comentários do que é realizado em cada etapa.

-  Para execução utilizei o Colab Enterprise no GCP, uma funcionalidade disponível no Vertex AI (Importante: para execução do script é necessária a habilitação do billing no projeto e é gerado custo para o usuário).

-  Existem diversas opções para carregar o notebook de execução, para os testes do desenvolvimento utilizei as opções de criar um notebook, importar do Google Cloud Storage e importar localmente da sua máquina.
![image](https://github.com/user-attachments/assets/135d996e-e9e7-4660-9ade-c3f5accabddd)

-  Versionamento: No próprio notebook existe a opção ‘Revision History’ onde caso todas as alterações sejam realizadas diretamente no mesmo notebook conseguimos verificar as modificações com horário de execução, conforme exemplo abaixo em que realizei um teste se o caminho para acesso das credenciais configuradas no Secret Manager estava correto.
  Particularmente prefiro o versionamento via Github pela facilidade de outros desenvolvedores poderem verificar as mudanças realizadas.
![image](https://github.com/user-attachments/assets/8e65e464-8438-4075-a807-dab7b023ed3a)

 





**Importante** - O passo a passo criado para execução hoje é realizado manualmente, existe a opção de agendarmos a execução do notebook porém a melhor alternativa seria automatizar toda a execução via Cloud Composer, utilizando o Apache Aiflow para criação e orquestração de dags com configuração de horário de execução, conexão com API e ingestão dos dados no BigQuery. Devido aos custos associados à ativação da API do Cloud Composer optei por apresentar a solução manual que pode facilmente ser adaptada para uma solução automatizada.

![image](https://github.com/user-attachments/assets/9e14a8d5-d2aa-424c-8662-1ad0a297124b)


![image](https://github.com/user-attachments/assets/02cd43a5-3b48-4baf-baa0-684079db5037)


![image](https://github.com/user-attachments/assets/6e8b3cfb-2f5c-47fe-86d1-37bc26da0348)


_**Observação**_: Consegui criar o processo de ingestão via Cloud Function, vincular um tópico do Pub/Sub como trigger porém a ingestão com sucesso foi realizada somente para a tabela 5. Quando a correção for finalizada e ingestão das três tabelas solicitadas realizada com sucesso atualizarei o código (main.py e requirements.txt) na pasta function_source nesse repositório.

![ingestao_function_tabela_5](https://github.com/user-attachments/assets/b90b7280-6633-4371-b141-0f5f24f7e5d9)



### Fluxo para dag:


![image](https://github.com/user-attachments/assets/aa411719-d702-4001-976a-c7696338e2cd)

