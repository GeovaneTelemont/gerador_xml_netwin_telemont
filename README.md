# 📋 Gerador XML Netwin Telemont

Este projeto é uma aplicação web para processar arquivos CSV e gerar arquivos XML organizados em um arquivo ZIP, conforme padrão Netwin/Telemont.

## ⚙ Funcionalidades

- Upload de arquivos CSV via interface web
- Processamento automático dos dados e geração de XMLs
- Mapeamento de códigos de complementos
- Conversão automática de coordenadas
- Download do arquivo ZIP com os XMLs organizados em pastas
- Log detalhado do processamento
- Interface amigável com Bootstrap

## 📌 Como usar

1. **Instale o Python:**  
   Baixe e instale o [Python 3.7+](https://www.python.org/downloads/).
   Baixe e instale o [Git](https://git-scm.com/downloads)

   ***Clone o projeto:***
   Abra o cmd ou terminal shell no windows:
   ```
      cd C:\Users\<usuário>\Desktop\
   ```
   Dentro do diretório:
   ```
      git clone https://github.com/GeovaneTelemont/gerador_xml_netwin_telemont.git
   ```
   Entra na pasta exemplo:
   ```
      cd C:\\C:\Users\<usuário>\Desktop\gerador_xml_netwin_telemont
   ```

2. **Crie um ambiente virtual dentro do projeto clonado (recomendado):**
   - Instale o virtualenv (caso não tenha):
     ```sh
     pip install virtualenv
     ```
   - Crie o ambiente virtual:
     ```sh
     python -m venv venv
     ```
   - Ative o ambiente virtual:
     - **Windows:**
       ```sh
       venv\Scripts\activate
       ```
     - **Linux/Mac:**
       ```sh
       source venv/bin/activate
       ```

3. **Instale as dependências do projeto:**
   - Usando o arquivo `requirements.txt`:
     ```sh
     pip install -r requirements.txt
     ```

4. **Execute o sistema:**
   ```sh
   python app.py
   ```

5. **Acesse [http://localhost:5000](http://localhost:5000) no navegador.**
6. Faça upload do arquivo CSV seguindo o modelo indicado na página inicial.
7. Após o processamento, faça o download do arquivo ZIP gerado.

## 💾 Estrutura do CSV

O arquivo CSV deve conter as seguintes colunas (separadas por ponto e vírgula):

- COMPLEMENTO, COMPLEMENTO2, RESULTADO
- LATITUDE, LONGITUDE, COD_ZONA
- LOCALIDADE, LOGRADOURO, BAIRRO
- MUNICIPIO, UF, COD_LOGRADOURO
- ID_ENDERECO, ID_ROTEIRO, ID_LOCALIDADE
- CEP, NUM_FACHADA, COD_SURVEY
- QUANTIDADE_UMS, UCS_RESIDENCIAIS, UCS_COMERCIAIS

## 📦 Requisitos

- Python 3.7+
- Flask
- pandas

## 📎 Observações

- Os arquivos gerados ficam disponíveis na pasta `downloads/` por até 1 hora.
- O sistema aceita arquivos CSV com codificação UTF-8, Latin-1, ISO-8859-1 ou CP1252.

## 🔗 Links úteis

- [Download Python](https://www.python.org/downloads/)
- [Documentação do virtualenv](https://virtualenv.pypa.io/en/latest/)
- [Documentação do Flask](https://flask.palletsprojects.com/)
- [Documentação do pandas](https://pandas.pydata.org/docs/)

## 🔒 Licença


Este projeto é privado e para uso interno Telemont.

