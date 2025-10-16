import pandas as pd
import xml.etree.ElementTree as ET
import os
import zipfile
from datetime import datetime
from flask import Flask, request, render_template, send_file, flash, redirect, url_for, session, Response, jsonify
from werkzeug.utils import secure_filename
import tempfile
import shutil
import time
import numpy as np
import re
import json
import threading
import queue

app = Flask(__name__)
app.secret_key = 'sua_chave_secreta_aqui'  # Altere para uma chave segura
app.config['UPLOAD_FOLDER'] = tempfile.mkdtemp()

# AUMENTAR DRAMATICAMENTE O LIMITE PARA 2GB
app.config['MAX_CONTENT_LENGTH'] = 2 * 1024 * 1024 * 1024  # 2GB max file size

# Configurar pasta de downloads
DOWNLOAD_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'downloads')
app.config['DOWNLOAD_FOLDER'] = DOWNLOAD_FOLDER

# Criar pasta de downloads se não existir
if not os.path.exists(DOWNLOAD_FOLDER):
    os.makedirs(DOWNLOAD_FOLDER)

# Dicionário de mapeamento de códigos de complemento
CODIGOS_COMPLEMENTO = {
    "AC": 1, "AA": 2, "AF": 3, "AL": 4, "AS": 5, "AB": 6, "AN": 7, "AX": 8,
    "AP": 9, "AZ": 10, "AT": 11, "BS": 12, "BA": 13, "BR": 14, "BC": 15,
    "BL": 16, "BX": 17, "CS": 18, "CM": 20, "CP": 21, "CA": 22, "CE": 23,
    "CT": 24, "CB": 25, "CL": 26, "CD": 27, "CJ": 28, "CR": 29, "CO": 30,
    "DP": 31, "DT": 32, "DV": 34, "ED": 35, "EN": 36, "ES": 37, "EC": 38,
    "ET": 39, "EP": 40, "FO": 42, "FR": 43, "FU": 44, "GL": 45, "GP": 46,
    "GA": 47, "GB": 48, "GJ": 49, "GR": 50, "GH": 52, "HG": 53, "LD": 55,
    "LM": 56, 'LH': 57, "LE": 58, "LJ": 59, "LT": 60, "LO": 61, "M": 62,
    "MT": 63, "MC": 64, "MZ": 65, "MD": 66, "NC": 67, "OM": 68, "OG": 69,
    "PC": 70, "PR": 71, "PP": 72, "PV": 73, "PM": 74, "PS": 75, "PA": 76,
    "PL": 77, "P": 78, "PO": 80, "PT": 81, "PD": 82, "PE": 83, "QU": 85,
    "QT": 86, "KM": 87, "QN": 88, "QQ": 89, "RM": 90, "RP": 91, "RF": 92,
    "RT": 93, "RL": 95, "SL": 96, "SC": 97, "SR": 98, "SB": 100, "SJ": 101,
    "SD": 102, "SU": 103, "SS": 104, "SQ": 105, "TN": 106, "TO": 107,
    "TE": 109, "TV": 110, "TR": 111, "VL": 112, "VZ": 113, "AD": 114,
    "BI": 115, "SA": 116, "NA": 117, "SK": 118, "ND": 119, "SE": 120,
    "AM": 121, "NR": 122, "CH": 124
}

# Colunas obrigatórias para validação
COLUNAS_OBRIGATORIAS = [
    'CELULA', 'ESTACAO_ABASTECEDORA', 'UF', 'MUNICIPIO', 'LOCALIDADE', 
    'COD_LOCALIDADE', 'LOCALIDADE_ABREV', 'LOGRADOURO', 'COD_LOGRADOURO', 
    'NUM_FACHADA', 'COMPLEMENTO', 'COMPLEMENTO2', 'COMPLEMENTO3', 'CEP', 
    'BAIRRO', 'COD_SURVEY', 'QUANTIDADE_UMS', 'COD_VIABILIDADE', 
    'TIPO_VIABILIDADE', 'TIPO_REDE', 'UCS_RESIDENCIAIS', 'UCS_COMERCIAIS', 
    'NOME_CDO', 'ID_ENDERECO', 'LATITUDE', 'LONGITUDE', 'TIPO_SURVEY', 
    'REDE_INTERNA', 'UMS_CERTIFICADAS', 'REDE_EDIF_CERT', 'DISP_COMERCIAL', 
    'ESTADO_CONTROLE', 'DATA_ESTADO_CONTROLE', 'ID_CELULA', 'QUANTIDADE_HCS'
]


LOG_COMPLEMENTOS = ""
ERRO_COMPLEMENTO3 = False
ERRO_COMPLEMENTO2 = False

# Sistema de mensagens para SSE
message_queue = queue.Queue()

# Dicionário global para armazenar resultados (em vez de session)
processing_results = {}
results_lock = threading.Lock()

progress_data = {
    'message': '',
    'progress': 0,
    'current': 0,
    'total': 0,
    'filename': '',
    'status': 'waiting'
}

progress_lock = threading.Lock()

def update_progress(message, progress=None, current=None, total=None, status=None):
    """Atualiza os dados de progresso e envia para a fila"""
    global progress_data
    with progress_lock:
        if message:
            progress_data['message'] = message
        if progress is not None:
            progress_data['progress'] = progress
        if current is not None:
            progress_data['current'] = current
        if total is not None:
            progress_data['total'] = total
        if status:
            progress_data['status'] = status
        
        # Envia cópia dos dados para a fila
        message_queue.put(progress_data.copy())

def validar_colunas_csv(arquivo_path):
    """Valida se o arquivo CSV contém todas as colunas obrigatórias"""
    try:
        # Tenta ler apenas o cabeçalho do CSV
        with open(arquivo_path, 'r', encoding='latin-1') as f:
            primeira_linha = f.readline().strip()
        
        # Verifica o separador (| ou ;)
        if '|' in primeira_linha:
            separador = '|'
        elif ';' in primeira_linha:
            separador = ';'
        else:
            separador = ','  # fallback
        
        # Lê apenas o cabeçalho
        df_header = pd.read_csv(arquivo_path, encoding='latin-1', sep=separador, nrows=0)
        colunas_encontradas = set(df_header.columns.str.strip().str.upper())
        colunas_obrigatorias_set = set([coluna.upper() for coluna in COLUNAS_OBRIGATORIAS])
        
        # Verifica colunas faltantes
        colunas_faltantes = colunas_obrigatorias_set - colunas_encontradas
        
        # Verifica se há colunas extras (opcional, apenas para informação)
        colunas_extras = colunas_encontradas - colunas_obrigatorias_set
        
        return {
            'valido': len(colunas_faltantes) == 0,
            'colunas_faltantes': list(colunas_faltantes),
            'colunas_extras': list(colunas_extras),
            'total_colunas': len(df_header.columns),
            'colunas_encontradas': list(colunas_encontradas)
        }
        
    except Exception as e:
        return {
            'valido': False,
            'erro': str(e),
            'colunas_faltantes': COLUNAS_OBRIGATORIAS,
            'colunas_extras': [],
            'total_colunas': 0,
            'colunas_encontradas': []
        }


def formatar_coordenada(coord):
    """Converte coordenada de formato brasileiro para internacional"""
    if pd.isna(coord):
        return None
    try:
        return float(str(coord).replace(',', '.'))
    except ValueError:
        return None

def obter_codigo_complemento(texto):
    """
    Obtém o código do complemento baseado nas duas primeiras letras do texto
    """
    if pd.isna(texto) or texto == '':
        return '60'  # Default para LT (LOTE)
    
    texto_str = str(texto).strip().upper()
    
    # Pegar as duas primeiras letras
    if len(texto_str) >= 2:
        codigo = texto_str[:2]
        return str(CODIGOS_COMPLEMENTO.get(codigo, 60))  # Default 60 se não encontrar
    else:
        return '60'  # Default para LT (LOTE)

def extrair_numero_argumento(texto):
    """
    Extrai TODO o conteúdo depois das duas primeiras letras
    """
    if pd.isna(texto) or texto == '':
        return '1'
    
    texto_str = str(texto).strip()
    
    if len(texto_str) < 2:
        return '1'
    
    argumento = texto_str[2:].strip()
    
    if argumento == '':
        return '1'
    
    return argumento

def determinar_destinacao(ucs_residenciais, ucs_comerciais):
    """Determina a destinação baseado nas UCs residenciais e comerciais"""
    if ucs_residenciais > 0 and ucs_comerciais == 0:
        return 'RESIDENCIA'
    elif ucs_comerciais > 0 and ucs_residenciais == 0:
        return 'COMERCIO'
    else:
        return 'MISTA'

def criar_xml_edificio_ccomplementos(dados_csv, numero_pasta, complemento_vazio):
    edificio = ET.Element('edificio')
    edificio.set('tipo', 'M')
    edificio.set('versao', '7.9.2')
    ET.SubElement(edificio, 'gravado').text = 'false'
    ET.SubElement(edificio, 'nEdificio').text = dados_csv['COD_SURVEY']
    latitude = formatar_coordenada(dados_csv['LATITUDE'])
    longitude = formatar_coordenada(dados_csv['LONGITUDE'])
    ET.SubElement(edificio, 'coordX').text = str(longitude)
    ET.SubElement(edificio, 'coordY').text = str(latitude)
    codigo_zona = str(dados_csv['COD_ZONA']) if 'COD_ZONA' in dados_csv and not pd.isna(dados_csv['COD_ZONA']) else 'DF-GURX-ETGR-CEOS-68'
    ET.SubElement(edificio, 'codigoZona').text = codigo_zona
    ET.SubElement(edificio, 'nomeZona').text = codigo_zona
    localidade = str(dados_csv['LOCALIDADE']) if 'LOCALIDADE' in dados_csv and not pd.isna(dados_csv['LOCALIDADE']) else 'GUARA'
    ET.SubElement(edificio, 'localidade').text = localidade
    endereco = ET.SubElement(edificio, 'enderecoEdificio')
    ET.SubElement(endereco, 'id').text = str(dados_csv['ID_ENDERECO']) if 'ID_ENDERECO' in dados_csv and not pd.isna(dados_csv['ID_ENDERECO']) else '93128133'
    logradouro = str(dados_csv['LOGRADOURO'] +", "+ dados_csv['BAIRRO']+", "+dados_csv['MUNICIPIO']+", "+dados_csv['LOCALIDADE']+" - "+ dados_csv["UF"]+ f" ({dados_csv['COD_LOGRADOURO']})" )
    ET.SubElement(endereco, 'logradouro').text = logradouro
    num_fachada = str(dados_csv['NUM_FACHADA']) if 'NUM_FACHADA' in dados_csv and not pd.isna(dados_csv['NUM_FACHADA']) else 'SN'
    ET.SubElement(endereco, 'numero_fachada').text = num_fachada
    complemento1 = dados_csv['COMPLEMENTO'] if 'COMPLEMENTO' in dados_csv else ''
    codigo_complemento1 = obter_codigo_complemento(complemento1)
    argumento1 = extrair_numero_argumento(complemento1)
    ET.SubElement(endereco, 'id_complemento1').text = codigo_complemento1
    ET.SubElement(endereco, 'argumento1').text = argumento1
    complemento2 = dados_csv['COMPLEMENTO2'] if 'COMPLEMENTO2' in dados_csv else ''
    codigo_complemento2 = obter_codigo_complemento(complemento2)
    argumento2 = extrair_numero_argumento(complemento2)
    ET.SubElement(endereco, 'id_complemento2').text = codigo_complemento2
    ET.SubElement(endereco, 'argumento2').text = argumento2

    # Só adiciona complemento3 se não estiver vazio
    if complemento_vazio == False:
        complemento3 = dados_csv['RESULTADO'] if 'RESULTADO' in dados_csv else ''
        if not pd.isna(complemento3) and str(complemento3).strip() != '':
            codigo_complemento3 = obter_codigo_complemento(complemento3)
            argumento3 = extrair_numero_argumento(complemento3)
            ET.SubElement(endereco, 'id_complemento3').text = codigo_complemento3
            ET.SubElement(endereco, 'argumento3').text = argumento3

    cep = str(dados_csv['CEP']) if 'CEP' in dados_csv and not pd.isna(dados_csv['CEP']) else '71065071'
    ET.SubElement(endereco, 'cep').text = cep
    bairro = str(dados_csv['BAIRRO']) if 'BAIRRO' in dados_csv and not pd.isna(dados_csv['BAIRRO']) else localidade
    ET.SubElement(endereco, 'bairro').text = bairro
    ET.SubElement(endereco, 'id_roteiro').text = str(dados_csv['ID_ROTEIRO']) if 'ID_ROTEIRO' in dados_csv and not pd.isna(dados_csv['ID_ROTEIRO']) else '57149008'
    ET.SubElement(endereco, 'id_localidade').text = str(dados_csv['ID_LOCALIDADE']) if 'ID_LOCALIDADE' in dados_csv and not pd.isna(dados_csv['ID_LOCALIDADE']) else '1894644'
    cod_lograd = str(dados_csv['COD_LOGRADOURO']) if 'COD_LOGRADOURO' in dados_csv and not pd.isna(dados_csv['COD_LOGRADOURO']) else '2700035341'
    ET.SubElement(endereco, 'cod_lograd').text = cod_lograd
    tecnico = ET.SubElement(edificio, 'tecnico')
    ET.SubElement(tecnico, 'id').text = '1828772688'
    ET.SubElement(tecnico, 'nome').text = 'NADIA CAROLINE'
    empresa = ET.SubElement(edificio, 'empresa')
    ET.SubElement(empresa, 'id').text = '42541126'
    ET.SubElement(empresa, 'nome').text = 'TELEMONT'
    data_atual = datetime.now().strftime('%Y%m%d%H%M%S')
    ET.SubElement(edificio, 'data').text = data_atual
    total_ucs = int(dados_csv['QUANTIDADE_UMS']) if 'QUANTIDADE_UMS' in dados_csv and not pd.isna(dados_csv['QUANTIDADE_UMS']) else 1
    ET.SubElement(edificio, 'totalUCs').text = str(total_ucs)
    ET.SubElement(edificio, 'ocupacao').text = "EDIFICACAOCOMPLETA"
    ET.SubElement(edificio, 'numPisos').text = '1'
    ET.SubElement(edificio, 'destinacao').text = 'COMERCIO'
    xml_str = ET.tostring(edificio, encoding='UTF-8', method='xml')
    xml_completo = b'<?xml version="1.0" encoding="UTF-8"?>' + xml_str
    return xml_completo 

def validador_xml(xml_content, complemento_vazio):
    pass
    
def processar_csv(arquivo_path):
    global LOG_COMPLEMENTOS
    global ERRO_COMPLEMENTO2
    global ERRO_COMPLEMENTO3
    ERRO_COMPLEMENTO2 = False
    ERRO_COMPLEMENTO3 = False


    try:
        encodings = ['utf-8', 'latin-1', 'iso-8859-1', 'cp1252']
        for encoding in encodings:
            try:
                df = pd.read_csv(arquivo_path, sep=';', encoding=encoding)
                # print(f"Arquivo lido com encoding: {encoding}")
                break 
            except UnicodeDecodeError:
                continue
        else:
            df = pd.read_csv(arquivo_path, sep=';')
    except Exception as e:
        raise Exception(f"Erro ao ler o arquivo CSV: {e}")

    if len(df) == 0:
        raise Exception("O arquivo CSV está vazio")

    estacao = df['ESTACAO_ABASTECEDORA'].iloc[0] if 'ESTACAO_ABASTECEDORA' in df.columns else 'DESCONHECIDA'
    diretorio_principal = f'moradias_xml_{estacao}_{datetime.now().strftime("%Y%m%d%H%M%S")}'
    os.makedirs(diretorio_principal, exist_ok=True)

    pastas_criadas = []
    log_processamento = []

    for i, (index, linha) in enumerate(df.iterrows(), 1):
         # Verifica se a coluna COMPLEMENTO3 e COMPLEMENTO2 estão totalmente vazias
        coluna_complemento_2_vazia = df['COMPLEMENTO3'].isna().all() or (df['COMPLEMENTO3'].astype(str).str.strip() == '').all()
        
        nome_pasta = f'moradia{i}'
        caminho_pasta = os.path.join(diretorio_principal, nome_pasta)
        os.makedirs(caminho_pasta, exist_ok=True)
        pastas_criadas.append(caminho_pasta)

        comp1 = linha['COMPLEMENTO'] if 'COMPLEMENTO' in linha else ''
        comp2 = linha['COMPLEMENTO2'] if 'COMPLEMENTO2' in linha else ''
        resultado = linha['RESULTADO'] if 'RESULTADO' in linha else ''

        xml_content = criar_xml_edificio_ccomplementos(linha, i, coluna_complemento_2_vazia)

        # validação dos complementos
        if comp1 == '' or pd.isna(comp1):
            ERRO_COMPLEMENTO2 = True
            LOG_COMPLEMENTOS = "⚠️(ERRO) no CSV na coluna do [COMPLEMENTO1], existem células que estão vazias. Todas as celulas da coluna COMPLEMENTO2 teve ser preenchidas para gerar o xml com 2 complementos."
        
        elif comp2 == '' or pd.isna(comp2):
            ERRO_COMPLEMENTO2 = True
            LOG_COMPLEMENTOS = "⚠️(ERRO) no CSV na coluna do [COMPLEMENTO2], existem células que estão vazias. Todas as celulas da coluna COMPLEMENTO2 teve ser preenchidas para gerar o xml com 2 complementos."

        elif resultado == '' or pd.isna(resultado):
            ERRO_COMPLEMENTO3 = True
            LOG_COMPLEMENTOS = "⚠️(ERRO) no CSV na coluna do [COMPLEMENTO3], existem células que estão vazias. Todas as celulas da coluna COMPLEMENTO3 teve ser preenchidas para gerar o xml com 3 complementos."
        
        elif coluna_complemento_2_vazia:
            ERRO_COMPLEMENTO3 = False
            ERRO_COMPLEMENTO2 = False
            LOG_COMPLEMENTOS = "✅(XML) com dois complementos gerado com sucesso! Agora é só fazer o download do zip!"
        else:
            ERRO_COMPLEMENTO3 = False
            ERRO_COMPLEMENTO2 = False
            LOG_COMPLEMENTOS = "✅(XML) com três complementos gerado com sucesso! Agora é só fazer o download do zip!"


        caminho_xml = os.path.join(caminho_pasta, f'{nome_pasta}.xml')
        with open(caminho_xml, 'wb') as f:
            f.write(xml_content)

        if i % 10 == 0 or i == 1:
            codigo1 = obter_codigo_complemento(comp1)
            codigo2 = obter_codigo_complemento(comp2)
            arg1 = extrair_numero_argumento(comp1)
            arg2 = extrair_numero_argumento(comp2)
            log_processamento.append(f'Registro {i}:')

            if coluna_complemento_2_vazia:
                log_processamento.append(f'  COMP1("{comp1}" → código:{codigo1} argumento:"{arg1}")')
                log_processamento.append(f'  COMP2("{comp2}" → código:{codigo2} argumento:"{arg2}")')
                log_processamento.append('-' * 50)
                
            else:
                codigo3 = obter_codigo_complemento(resultado)
                arg3 = extrair_numero_argumento(resultado)
                log_processamento.append(f'  COMP1("{comp1}" → código:{codigo1} argumento:"{arg1}")')
                log_processamento.append(f'  COMP2("{comp2}" → código:{codigo2} argumento:"{arg2}")')
                log_processamento.append(f'  COMP3("{resultado}" → código:{codigo3} argumento:"{arg3}")')
                log_processamento.append('-' * 50)
          

    zip_filename = os.path.join(app.config['DOWNLOAD_FOLDER'], f'{diretorio_principal}.zip')
    with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for pasta in pastas_criadas:
            for root, dirs, files in os.walk(pasta):
                for file in files:
                    file_path = os.path.join(root, file)
                    arcname = os.path.relpath(file_path, diretorio_principal)
                    zipf.write(file_path, arcname)

    shutil.rmtree(diretorio_principal)
    return os.path.basename(zip_filename), len(df), '\n'.join(log_processamento) 

   

def limpar_arquivos_antigos():
    """Limpa arquivos com mais de 1 hora na pasta de downloads"""
    try:
        agora = time.time()
        for filename in os.listdir(app.config['DOWNLOAD_FOLDER']):
            file_path = os.path.join(app.config['DOWNLOAD_FOLDER'], filename)
            if os.path.isfile(file_path):
                # Verificar se o arquivo tem mais de 1 hora
                if agora - os.path.getctime(file_path) > 3600:
                    os.remove(file_path)
    except Exception as e:
        print(f"Erro ao limpar arquivos antigos: {e}")

# ========== NOVAS FUNÇÕES PARA CONVERSÃO DE CSV ==========

def carregar_roteiros():
    """Carrega os arquivos de roteiro necessários para a conversão"""
    try:
        # Caminho absoluto para a pasta de roteiros
        base_dir = os.path.dirname(os.path.abspath(__file__))
        roteiros_dir = os.path.join(base_dir, 'roteiros')
        
        caminho_aparecida = os.path.join(roteiros_dir, 'roteiro_aparecida.xlsx')
        caminho_goiania = os.path.join(roteiros_dir, 'roteiro_goiania.xlsx')
        
        print(f"📂 Tentando carregar roteiros de:")
        print(f"   - {caminho_aparecida}")
        print(f"   - {caminho_goiania}")
        
        # Verificar se os arquivos existem
        if not os.path.exists(caminho_aparecida):
            print(f"❌ Arquivo não encontrado: {caminho_aparecida}")
            return None, None
        if not os.path.exists(caminho_goiania):
            print(f"❌ Arquivo não encontrado: {caminho_goiania}")
            return None, None
            
        df_roteiro_aparecida = pd.read_excel(caminho_aparecida)
        df_roteiro_goiania = pd.read_excel(caminho_goiania)
        
        print("✅ Roteiros carregados com sucesso")
        print(f"   - Aparecida: {len(df_roteiro_aparecida)} registros")
        print(f"   - Goiania: {len(df_roteiro_goiania)} registros")
        
        return df_roteiro_aparecida, df_roteiro_goiania
        
    except Exception as e:
        print(f"❌ Erro ao carregar roteiros: {e}")
        return None, None

def processar_enderecos_otimizado(df_enderecos, df_roteiro_aparecida, df_roteiro_goiania):
    """
    Processa os dados de endereços de forma otimizada mantendo TODOS os dados originais
    """
    
    # Fazer uma cópia do dataframe
    df = df_enderecos.copy()
    
    print(f"🔍 Colunas iniciais: {list(df.columns)}")
    print(f"📊 Total de linhas inicial: {len(df)}")
    
    # ========== CORREÇÕES DE FORMATAÇÃO OTIMIZADAS ==========
    
    print("🔧 Aplicando correções de formatação...")
    
    # 1. Corrigir CEP - operações vetorizadas
    if 'CEP' in df.columns:
        df['CEP'] = (df['CEP'].astype(str)
                      .str.strip()
                      .str.replace(r'\D', '', regex=True)
                      .str[:8]
                      .apply(lambda x: x.zfill(8) if x != '' else ''))
        print("✅ CEP formatado")
    
    # 2. Corrigir COD_LOGRADOURO - operações vetorizadas
    if 'COD_LOGRADOURO' in df.columns:
        df['COD_LOGRADOURO'] = (df['COD_LOGRADOURO'].astype(str)
                                 .str.strip()
                                 .str.replace(r'\D', '', regex=True)
                                 .str[:10])
        print("✅ COD_LOGRADOURO formatado")
    
    # ========== CRIAÇÃO DE COLUNAS OTIMIZADAS ==========
    
    print("Criando CHAVE LOG...")
    # CHAVE LOG otimizada - evita apply
    colunas_chave = ['ESTACAO_ABASTECEDORA', 'LOCALIDADE', 'LOGRADOURO', 'COMPLEMENTO', 'COMPLEMENTO2']
    for coluna in colunas_chave:
        if coluna in df.columns:
            df[coluna] = df[coluna].fillna('').astype(str).str.strip()
    
    # Cria CHAVE LOG de forma vetorizada
    df['CHAVE LOG'] = (df['ESTACAO_ABASTECEDORA'] + "-" + 
                      df['LOCALIDADE'] + "-" + 
                      df['LOGRADOURO'] + "-" + 
                      df['COMPLEMENTO'] + "-" + 
                      df['COMPLEMENTO2'])
    
    # Remove hífens extras
    df['CHAVE LOG'] = df['CHAVE LOG'].str.replace(r'-+', '-', regex=True).str.strip('-')
    
    print("Processando COMPLEMENTO3...")
    # COMPLEMENTO3 otimizado - MANTÉM OS DADOS ORIGINAIS
    if 'COMPLEMENTO3' in df.columns:
        # Salva o original
        df['COMPLEMENTO3_ORIGINAL'] = df['COMPLEMENTO3']
        # Cria versão tratada para processamento
        df['COMPLEMENTO3_TRATADO'] = df['COMPLEMENTO3'].fillna('').astype(str).str.strip().str.upper()
    else:
        df['COMPLEMENTO3_ORIGINAL'] = ''
        df['COMPLEMENTO3_TRATADO'] = ''
    
    # Extrai prefixo de forma vetorizada (usa o tratado)
    df['Prefixo'] = df['COMPLEMENTO3_TRATADO'].str[:2]
    
    print("Agrupando e numerando...")
    # Filtra e agrupa de forma mais eficiente - MAS MANTÉM TODAS AS LINHAS
    mask_prefixo_valido = (df['Prefixo'].notna()) & (df['Prefixo'] != "")
    df_com_prefixo = df[mask_prefixo_valido].copy()
    df_sem_prefixo = df[~mask_prefixo_valido].copy()
    
    # Aplica agrupamento apenas se houver dados
    if len(df_com_prefixo) > 0:
        df_com_prefixo['ORDEM'] = df_com_prefixo.groupby(['CHAVE LOG', 'Prefixo']).cumcount() + 1
        df_com_prefixo['Resultado'] = df_com_prefixo['Prefixo'] + " " + df_com_prefixo['ORDEM'].astype(str)
        df_com_prefixo = df_com_prefixo.drop('Prefixo', axis=1)
    else:
        df_com_prefixo['ORDEM'] = 0
        df_com_prefixo['Resultado'] = ""
    
    # Prepara dados sem prefixo - MANTÉM TODOS OS DADOS ORIGINAIS
    df_sem_prefixo['ORDEM'] = 0
    df_sem_prefixo['Resultado'] = ""
    if 'Prefixo' in df_sem_prefixo.columns:
        df_sem_prefixo = df_sem_prefixo.drop('Prefixo', axis=1)
    
    # Combina os dataframes - AGORA COM TODAS AS LINHAS
    df = pd.concat([df_com_prefixo, df_sem_prefixo], ignore_index=True)
    
    print(f"📊 Linhas com prefixo válido: {len(df_com_prefixo)}")
    print(f"📊 Linhas sem prefixo válido: {len(df_sem_prefixo)}")
    print(f"📊 Total após concatenação: {len(df)}")
    
    # ========== TRANSFORMAÇÕES RÁPIDAS ==========
    
    print("Aplicando transformações rápidas...")
    
    # COD_ZONA otimizado
    if 'CELULA' in df.columns:
        df['Nº CELULA'] = df['CELULA'].str.split(' ').str[0].fillna('')
    else:
        df['Nº CELULA'] = ''
    
    df['COD_ZONA'] = (df['UF'] + "-" + df['LOCALIDADE_ABREV'] + "-" + 
                     df['ESTACAO_ABASTECEDORA'] + "-CEOS-" + df['Nº CELULA'])
    
    # RESULTADO e COMPARATIVO otimizados
    df['RESULTADO'] = df['Resultado'].str.replace(' ', '')
    
    # COMPARATIVO usa o COMPLEMENTO3 original
    df['COMPARATIVO'] = np.where(df['RESULTADO'] == df['COMPLEMENTO3_TRATADO'], "VERDADEIRO", "FALSO")
    
    # Remove coluna temporária
    if 'Nº CELULA' in df.columns:
        df = df.drop('Nº CELULA', axis=1)
    
    # ========== MERGE OTIMIZADO E CORRIGIDO ==========
    
    print("Fazendo merge com roteiros...")
    df_roteiros = pd.concat([df_roteiro_aparecida, df_roteiro_goiania], ignore_index=True)
    
    # Prepara colunas para merge - CORREÇÃO DO ERRO
    if 'id' in df_roteiros.columns:
        df_roteiros['id'] = df_roteiros['id'].astype(str).str.replace(r'\.0$', '', regex=True)
    if 'id_localidade' in df_roteiros.columns:
        df_roteiros['id_localidade'] = df_roteiros['id_localidade'].astype(str).str.replace(r'\.0$', '', regex=True)
    
    # CORREÇÃO: Converter cod_lograd para string para compatibilidade com COD_LOGRADOURO
    if 'cod_lograd' in df_roteiros.columns:
        df_roteiros['cod_lograd'] = df_roteiros['cod_lograd'].astype(str).str.strip()
        df_roteiros['cod_lograd'] = df_roteiros['cod_lograd'].str.replace(r'\D', '', regex=True)
        df_roteiros['cod_lograd'] = df_roteiros['cod_lograd'].str[:10]  # Garante 10 dígitos
    
    # Faz merge apenas se as colunas existem
    if 'COD_LOGRADOURO' in df.columns and 'cod_lograd' in df_roteiros.columns:
        # Garantir que ambas as colunas são strings
        df['COD_LOGRADOURO'] = df['COD_LOGRADOURO'].astype(str)
        df_roteiros['cod_lograd'] = df_roteiros['cod_lograd'].astype(str)
        
        df = df.merge(
            df_roteiros[['cod_lograd', 'id', 'id_localidade']],
            left_on='COD_LOGRADOURO',
            right_on='cod_lograd',
            how='left'
        )
        df = df.rename(columns={'id': 'ID_ROTEIRO', 'id_localidade': 'ID_LOCALIDADE'})
        if 'cod_lograd' in df.columns:
            df = df.drop('cod_lograd', axis=1)
        print("✅ Merge com roteiros concluído")
    else:
        df['ID_ROTEIRO'] = ''
        df['ID_LOCALIDADE'] = ''
        print("⚠️  Merge não realizado - colunas de junção não encontradas")
    
    # ========== REMOVE DUPLICATAS ==========
    
    if 'COD_SURVEY' in df.columns:
        antes = len(df)
        df = df.drop_duplicates(subset=['COD_SURVEY'])
        depois = len(df)
        print(f"📊 Duplicatas removidas: {antes - depois}")
    
    # ========== COLUNAS NUMÉRICAS ==========
    
    print("Processando colunas numéricas...")
    # Extrai números do COMPLEMENTO3 tratado
    df['Nº ARGUMENTO3 COMPLEMENTO3'] = (df['COMPLEMENTO3_TRATADO']
                                       .str.extract(r'(\d+)')[0]
                                       .fillna(0)
                                       .astype(int))
    
    df['ORDEM'] = df['ORDEM'].astype(int)
    
    # ========== VALIDAÇÃO SIMPLIFICADA ==========
    
    print("Criando validação...")
    conditions = [
        df['ORDEM'] == 0,
        df['Nº ARGUMENTO3 COMPLEMENTO3'] == 0,
        df['Nº ARGUMENTO3 COMPLEMENTO3'] > 10,
        df['ORDEM'] > 10
    ]
    
    choices = [
        "SEM PREFIXO VÁLIDO",
        "VERIFICAR COMPLEMENTO3-VAZIO",
        "VERIFICAR COMPLEMENTO3 >10", 
        "VERIFICAR RESULTADO >10"
    ]
    
    df['VALIDAÇÃO'] = np.select(conditions, choices, default="OK")
    
    # ========== GARANTIR ESTRUTURA FINAL ==========
    
    print("Finalizando estrutura...")
    
    # RESTAURA O COMPLEMENTO3 ORIGINAL
    df['COMPLEMENTO3'] = df['COMPLEMENTO3_ORIGINAL']
    
    colunas_finais = [
        'CHAVE LOG', 'CELULA', 'ESTACAO_ABASTECEDORA', 'UF', 'MUNICIPIO', 'LOCALIDADE', 
        'COD_LOCALIDADE', 'LOCALIDADE_ABREV', 'LOGRADOURO', 'COD_LOGRADOURO', 'NUM_FACHADA', 
        'COMPLEMENTO', 'COMPLEMENTO2', 'COMPLEMENTO3', 'CEP', 'BAIRRO', 'COD_SURVEY', 
        'QUANTIDADE_UMS', 'COD_VIABILIDADE', 'TIPO_VIABILIDADE', 'TIPO_REDE', 'UCS_RESIDENCIAIS', 
        'UCS_COMERCIAIS', 'NOME_CDO', 'ID_ENDERECO', 'LATITUDE', 'LONGITUDE', 'TIPO_SURVEY', 
        'REDE_INTERNA', 'UMS_CERTIFICADAS', 'REDE_EDIF_CERT', 'DISP_COMERCIAL', 'ESTADO_CONTROLE', 
        'DATA_ESTADO_CONTROLE', 'ID_CELULA', 'QUANTIDADE_HCS', 'ID_ROTEIRO', 'ID_LOCALIDADE', 
        'COD_ZONA', 'ORDEM', 'RESULTADO', 'COMPARATIVO', 'Nº ARGUMENTO3 COMPLEMENTO3', 'VALIDAÇÃO'
    ]
    
    # Adiciona colunas faltantes
    for coluna in colunas_finais:
        if coluna not in df.columns:
            df[coluna] = ''
    
    # Reordena colunas
    df = df[colunas_finais]
    
    # Remove colunas auxiliares
    colunas_para_remover = ['COMPLEMENTO3_ORIGINAL', 'COMPLEMENTO3_TRATADO', 'Resultado']
    for coluna in colunas_para_remover:
        if coluna in df.columns:
            df = df.drop(coluna, axis=1)
    
    # ========== PREPARAÇÃO PARA POWER QUERY (RÁPIDA) ==========
    
    print("Preparando para Power Query...")
    
    # Apenas as correções essenciais para Power Query
    df = df.replace({
        'NaN': '',
        'nan': '',
        'None': '',
        'null': '',
        'True': 'VERDADEIRO',
        'False': 'FALSO'
    })
    
    # Remove valores nulos
    df = df.fillna('')
    
    print(f"✅ Processamento concluído. Linhas: {len(df):,}")
    
    # Verificar estatísticas dos dados
    print(f"\n📈 ESTATÍSTICAS DOS DADOS:")
    print(f"   - Total de linhas: {len(df):,}")
    print(f"   - COMPLEMENTO3 vazios: {(df['COMPLEMENTO3'] == '').sum():,}")
    print(f"   - COMPLEMENTO2 vazios: {(df['COMPLEMENTO2'] == '').sum():,}")
    print(f"   - Linhas com validação OK: {(df['VALIDAÇÃO'] == 'OK').sum():,}")
    print(f"   - Linhas sem prefixo válido: {(df['VALIDAÇÃO'] == 'SEM PREFIXO VÁLIDO').sum():,}")
    
    return df

def processar_csv_conversor(arquivo_path):
    """Processa o arquivo CSV para conversão"""
    try:
        print(f"📂 Carregando {arquivo_path}...")
        
        # Carrega o CSV
        df_enderecos = pd.read_csv(
            arquivo_path,
            encoding='latin-1',
            sep='|',
            engine='c',
            low_memory=False
        )
        
        print(f"✅ CSV carregado: {len(df_enderecos):,} linhas")
        
        # Carrega os roteiros
        df_roteiro_aparecida, df_roteiro_goiania = carregar_roteiros()
        if df_roteiro_aparecida is None or df_roteiro_goiania is None:
            raise Exception("Erro ao carregar arquivos de roteiro. Verifique se os arquivos estão na pasta 'roteiros'.")
        
        # Processa os dados
        df_final = processar_enderecos_otimizado(df_enderecos, df_roteiro_aparecida, df_roteiro_goiania)
        
        # Gera nome do arquivo
        nome_arquivo = f"Enderecos_Totais_CO_Convertido_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv"
        caminho_arquivo = os.path.join(app.config['DOWNLOAD_FOLDER'], nome_arquivo)
        
        # Salva o arquivo
        df_final.to_csv(
            caminho_arquivo,
            index=False,
            encoding='utf-8-sig',
            sep=';',
            quoting=1,
            quotechar='"',
            na_rep=''
        )
        
        print(f"✅ Arquivo convertido salvo: {nome_arquivo}")
        return nome_arquivo, len(df_final)
        
    except Exception as e:
        print(f"❌ Erro no processamento: {e}")
        raise Exception(f"Erro ao processar arquivo: {str(e)}")

# ========== FUNÇÃO OTIMIZADA COM PROGRESSO ==========

def processar_csv_conversor_grande(arquivo_path):
    """Processa o arquivo CSV para conversão - OTIMIZADO PARA ARQUIVOS GRANDES"""
    try:
        update_progress("📂 Iniciando carregamento do arquivo...", progress=5, status='processing')
        
        # Verificar tamanho do arquivo
        file_size = os.path.getsize(arquivo_path) / (1024 * 1024)  # Tamanho em MB
        update_progress(f"📊 Tamanho do arquivo: {file_size:.2f} MB", progress=10)
        
        # Carrega os roteiros primeiro (uma vez só)
        update_progress("📁 Carregando arquivos de roteiro...", progress=15)
        df_roteiro_aparecida, df_roteiro_goiania = carregar_roteiros()
        if df_roteiro_aparecida is None or df_roteiro_goiania is None:
            raise Exception("Erro ao carregar arquivos de roteiro. Verifique se os arquivos estão na pasta 'roteiros'.")
        
        update_progress("✅ Roteiros carregados com sucesso", progress=20)

        # Processamento em chunks para arquivos grandes
        chunk_size = 50000  # Ajuste conforme a memória disponível
        chunks_processed = 0
        total_rows = 0
        
        # Primeiro passagem: contar linhas totais
        update_progress("🔢 Contando linhas totais...", progress=25)
        with open(arquivo_path, 'r', encoding='latin-1') as f:
            total_rows = sum(1 for line in f) - 1  # -1 para o cabeçalho
        
        update_progress(f"📊 Total de linhas encontradas: {total_rows:,}", progress=30, total=total_rows)
        
        # Lista para armazenar chunks processados
        chunks_processados = []
        
        # Processar em chunks
        update_progress("🔄 Iniciando processamento em chunks...", progress=35)
        
        for chunk_number, chunk in enumerate(pd.read_csv(arquivo_path, 
                                encoding='latin-1',
                                sep='|',
                                chunksize=chunk_size,
                                low_memory=False), 1):
            
            chunks_processed += 1
            current_row = chunk_number * chunk_size
            if current_row > total_rows:
                current_row = total_rows
                
            progress_percent = 35 + (chunk_number * 55 / (total_rows / chunk_size))
            progress_percent = min(progress_percent, 90)
            
            update_progress(
                f"📦 Processando chunk {chunk_number} ({len(chunk):,} linhas)...", 
                progress=progress_percent,
                current=current_row
            )
            
            # Processa o chunk
            chunk_processado = processar_enderecos_otimizado(chunk, df_roteiro_aparecida, df_roteiro_goiania)
            chunks_processados.append(chunk_processado)
            
            # Limpar memória
            del chunk
            del chunk_processado
            
            update_progress(f"✅ Chunk {chunk_number} processado", progress=progress_percent)
        
        # Combinar todos os chunks
        update_progress("🔗 Combinando chunks processados...", progress=92)
        df_final = pd.concat(chunks_processados, ignore_index=True)
        
        # Gera nome do arquivo
        nome_arquivo = f"Enderecos_Totais_CO_Convertido_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv"
        caminho_arquivo = os.path.join(app.config['DOWNLOAD_FOLDER'], nome_arquivo)
        
        # Salva o arquivo em chunks também (para evitar problemas de memória)
        update_progress("💾 Salvando arquivo final...", progress=95)
        df_final.to_csv(
            caminho_arquivo,
            index=False,
            encoding='utf-8-sig',
            sep=';',
            quoting=1,
            quotechar='"',
            na_rep='',
            chunksize=10000  # Salva em chunks também
        )
        
        update_progress(
            f"✅ Conversão concluída! Arquivo salvo: {nome_arquivo}", 
            progress=100, 
            current=total_rows,
            status='completed'
        )
        
        print(f"✅ Arquivo convertido salvo: {nome_arquivo}")
        print(f"📊 Total processado: {len(df_final):,} linhas")
        
        return nome_arquivo, len(df_final)
        
    except Exception as e:
        error_msg = f"❌ Erro no processamento: {str(e)}"
        update_progress(error_msg, status='error')
        print(error_msg)
        import traceback
        print(f"📋 Traceback: {traceback.format_exc()}")
        raise Exception(f"Erro ao processar arquivo: {str(e)}")

# ========== ROTAS EXISTENTES ==========

@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        if 'file' not in request.files:
            flash('Nenhum arquivo selecionado')
            return redirect(request.url)
        
        file = request.files['file']
        if file.filename == '':
            flash('Nenhum arquivo selecionado')
            return redirect(request.url)
        
        if file and file.filename.endswith('.csv'):
            filename = secure_filename(file.filename)
            filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            file.save(filepath)
            
            try:
                zip_filename, total_registros, log = processar_csv(filepath)
                flash(f'Processamento concluído! {total_registros} registros processados.')
                
                if ERRO_COMPLEMENTO2 or ERRO_COMPLEMENTO3:
                    alert_type = "danger"
                else:
                    alert_type = "info"
                 
                return render_template('resultado.html', 
                                    complementos = LOG_COMPLEMENTOS,
                                    alert_type=alert_type,
                                    log=log, 
                                    total_registros=total_registros,
                                    zip_filename=zip_filename)
                
            except Exception as e:
                flash(f'Erro no processamento: {str(e)}')
                return redirect(request.url)
            
            finally:
                # Limpar arquivo temporário
                if os.path.exists(filepath):
                    os.remove(filepath)
        else:
            flash('Por favor, selecione um arquivo CSV')
            return redirect(request.url)
    
    return render_template('index.html')

@app.route('/download/<filename>')
def download_file(filename):
    try:
        file_path = os.path.join(app.config['DOWNLOAD_FOLDER'], filename)
        
        # Verificar se o arquivo existe
        if not os.path.exists(file_path):
            flash('Arquivo não encontrado')
            return redirect(url_for('index'))
        
        # Enviar o arquivo para download
        return send_file(
            file_path,
            as_attachment=True,
            download_name=filename,
            mimetype='application/zip'
        )
    
    except Exception as e:
        flash(f'Erro ao fazer download: {str(e)}')
        return redirect(url_for('index'))

@app.route('/sobre')
def sobre():
    return render_template('sobre.html')

@app.route('/download-modelo-csv')
def download_modelo_csv():
    modelo_path = os.path.join(os.path.dirname(__file__), 'csv_modelo', 'modelo.csv')
    if not os.path.exists(modelo_path):
        flash('Arquivo modelo não encontrado.')
        return redirect(url_for('index'))
    return send_file(
        modelo_path,
        as_attachment=True,
        download_name='modelo.csv',
        mimetype='text/csv'
    )

# ========== NOVAS ROTAS COM PROGRESSO ==========

@app.route('/progress')
def progress():
    """Rota para SSE do progresso - CORRIGIDA"""
    def generate():
        try:
            # Envia um ping inicial para manter a conexão
            yield f"data: {json.dumps({'message': 'Conectado...', 'status': 'connected'})}\n\n"
            
            last_data = None
            while True:
                try:
                    # Pega a mensagem mais recente da fila (com timeout)
                    data = message_queue.get(timeout=30)
                    
                    # Só envia se os dados mudaram
                    if data != last_data:
                        yield f"data: {json.dumps(data)}\n\n"
                        last_data = data
                        
                        # Se o processamento terminou, encerra a conexão
                        if data.get('status') in ['completed', 'error']:
                            break
                    
                    message_queue.task_done()
                    
                except queue.Empty:
                    # Timeout - envia ping para manter conexão
                    yield f"data: {json.dumps({'message': 'Aguardando...', 'status': 'waiting'})}\n\n"
                    
        except GeneratorExit:
            # Cliente desconectou
            print("Cliente desconectou do SSE")
        except Exception as e:
            print(f"Erro no SSE: {e}")
            yield f"data: {json.dumps({'message': f'Erro: {str(e)}', 'status': 'error'})}\n\n"
    
    return Response(
        generate(),
        mimetype='text/event-stream',
        headers={
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Headers': 'Cache-Control'
        }
    )

@app.route('/validar-csv', methods=['POST'])
def validar_csv():
    """Rota para validar o arquivo CSV via AJAX"""
    if 'file' not in request.files:
        return jsonify({'valido': False, 'erro': 'Nenhum arquivo enviado'})
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({'valido': False, 'erro': 'Nenhum arquivo selecionado'})
    
    if file and file.filename.endswith('.csv'):
        try:
            # Salva o arquivo temporariamente
            filename = secure_filename(file.filename)
            filepath = os.path.join(app.config['UPLOAD_FOLDER'], f"temp_{filename}")
            file.save(filepath)
            
            # Valida as colunas
            resultado_validacao = validar_colunas_csv(filepath)
            
            # Limpa o arquivo temporário
            if os.path.exists(filepath):
                os.remove(filepath)
            
            return jsonify(resultado_validacao)
            
        except Exception as e:
            # Limpa o arquivo temporário em caso de erro
            if os.path.exists(filepath):
                os.remove(filepath)
            return jsonify({'valido': False, 'erro': f'Erro na validação: {str(e)}'})
    
    return jsonify({'valido': False, 'erro': 'Arquivo inválido'})

@app.route('/conversor-csv', methods=['GET', 'POST'])
def conversor_csv():
    if request.method == 'POST':
        if 'file' not in request.files:
            flash('Nenhum arquivo selecionado')
            return redirect(request.url)
        
        file = request.files['file']
        if file.filename == '':
            flash('Nenhum arquivo selecionado')
            return redirect(request.url)
        
        if file and file.filename.endswith('.csv'):
            filename = secure_filename(file.filename)
            filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            file.save(filepath)
            
            # Valida o arquivo antes de processar
            validacao = validar_colunas_csv(filepath)
            if not validacao['valido']:
                colunas_faltantes = ', '.join(validacao['colunas_faltantes'])
                flash(f'❌ Arquivo inválido! Colunas faltantes: {colunas_faltantes}')
                if os.path.exists(filepath):
                    os.remove(filepath)
                return redirect(request.url)
            
            # Reset progress data
            update_progress('🕒 Iniciando processamento...', progress=0, current=0, total=0, status='processing')
            
            try:
                # Verificar tamanho do arquivo
                file_size = os.path.getsize(filepath) / (1024 * 1024)
                
                # Limpar a fila de mensagens antigas
                while not message_queue.empty():
                    try:
                        message_queue.get_nowait()
                        message_queue.task_done()
                    except queue.Empty:
                        break
                
                # Gerar um ID único para este processamento
                import uuid
                process_id = str(uuid.uuid4())
                
                # Iniciar processamento em thread separada
                def processar_arquivo(process_id, filepath, file_size):
                    try:
                        update_progress(f'📊 Arquivo validado: {file_size:.2f} MB', progress=5)
                        
                        if file_size > 100:
                            update_progress('🔧 Usando processamento otimizado para arquivo grande...', progress=10)
                            zip_filename, total_registros = processar_csv_conversor_grande(filepath)
                        else:
                            update_progress('🔧 Processando arquivo...', progress=10)
                            zip_filename, total_registros = processar_csv_conversor(filepath)
                        
                        # Armazenar resultado no dicionário global
                        with results_lock:
                            processing_results[process_id] = {
                                'filename': zip_filename,
                                'total_registros': total_registros,
                                'status': 'success'
                            }
                        
                        update_progress('✅ Processamento concluído com sucesso!', progress=100, status='completed')
                        
                    except Exception as e:
                        error_msg = f'❌ Erro no processamento: {str(e)}'
                        print(error_msg)
                        
                        # Armazenar erro no dicionário global
                        with results_lock:
                            processing_results[process_id] = {
                                'error': str(e),
                                'status': 'error'
                            }
                        
                        update_progress(error_msg, status='error')
                    finally:
                        # Limpar arquivo temporário
                        if os.path.exists(filepath):
                            os.remove(filepath)
                
                thread = threading.Thread(target=processar_arquivo, args=(process_id, filepath, file_size))
                thread.daemon = True
                thread.start()
                
                # Armazenar o process_id na session para recuperar depois
                session['current_process_id'] = process_id
                
                return redirect(url_for('progress_page'))
                
            except Exception as e:
                flash(f'❌ Erro ao iniciar processamento: {str(e)}')
                if os.path.exists(filepath):
                    os.remove(filepath)
                return redirect(request.url)
        else:
            flash('Por favor, selecione um arquivo CSV')
            return redirect(request.url)
    
    return render_template('conversor_csv.html')

    
@app.route('/progress-page')
def progress_page():
    """Página que mostra o progresso"""
    return render_template('progresso.html')

@app.route('/conversor-result')
def conversor_result():
    """Página de resultado após processamento"""
    process_id = session.get('current_process_id')
    
    if not process_id:
        flash('Nenhum processamento em andamento')
        return redirect(url_for('conversor_csv'))
    
    with results_lock:
        result = processing_results.get(process_id)
    
    if not result:
        flash('Resultado não encontrado. O processamento pode ainda estar em andamento.')
        return redirect(url_for('conversor_csv'))
    
    if result.get('status') == 'success':
        # Limpar o resultado após usar
        with results_lock:
            processing_results.pop(process_id, None)
        session.pop('current_process_id', None)
        
        return render_template('resultado_conversor.html', 
                            total_registros=result['total_registros'],
                            zip_filename=result['filename'])
    
    elif result.get('status') == 'error':
        error_msg = result.get('error', 'Erro desconhecido')
        # Limpar o resultado após usar
        with results_lock:
            processing_results.pop(process_id, None)
        session.pop('current_process_id', None)
        
        flash(f'❌ Erro na conversão: {error_msg}')
        return redirect(url_for('conversor_csv'))
    
    else:
        flash('Processamento ainda em andamento...')
        return redirect(url_for('progress_page'))

@app.route('/download-convertido/<filename>')
def download_convertido(filename):
    try:
        file_path = os.path.join(app.config['DOWNLOAD_FOLDER'], filename)
        
        # Verificar se o arquivo existe
        if not os.path.exists(file_path):
            flash('Arquivo não encontrado')
            return redirect(url_for('conversor_csv'))
        
        # Enviar o arquivo para download
        return send_file(
            file_path,
            as_attachment=True,
            download_name=filename,
            mimetype='text/csv'
        )
    
    except Exception as e:
        flash(f'Erro ao fazer download: {str(e)}')
        return redirect(url_for('conversor_csv'))

# Adicionar tratamento de erro para arquivos grandes
@app.errorhandler(413)
def too_large(e):
    flash('O arquivo é muito grande. O tamanho máximo permitido é 2GB.')
    return redirect(request.url)

# ========== FUNÇÃO CRIAR TEMPLATES COMPLETA ==========

def criar_templates():
    templates_dir = os.path.join(os.path.dirname(__file__), 'templates')
    
    # Template index.html
    index_html = '''<!DOCTYPE html>
<html lang="pt-BR">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Gerador de XML para Edificações</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/css/bootstrap.min.css" rel="stylesheet">
    <!-- Link para Font Awesome -->
    <link href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/5.15.4/css/all.min.css" rel="stylesheet">
    <link rel="icon" type="image/x-icon" href="{{ url_for('static', filename='img/telemont.ico') }}">
    <style>
        .container { max-width: 800px; }
        .upload-box { border: 2px dashed #ccc; padding: 2rem; text-align: center; }
        .btn-custom { background-color: #0d6efd; color: white; }
        .card { transition: transform 0.2s; }
        .card:hover { transform: translateY(-2px); }
    </style>
</head>
<body>
    <div class="container mt-5">
        <div class="row">
            <div class="col-12 text-center">
                <img src="{{ url_for('static', filename='img/telemont.png') }}" alt="Logo Telemont" class="img-fluid p-3" style="width: 300px; display: block; margin: 0px auto;">
                <h1 class="mb-4">📁 Gerador de XML para Edificações</h1>
                <p class="lead">Faça upload de um arquivo CSV para gerar arquivos XML</p>
            </div>
        </div>

        <div class="row mt-4">
            <div class="col-12">
                <div class="upload-box rounded-3">
                    <form method="POST" enctype="multipart/form-data">
                        <div class="mb-3">
                            <label for="file" class="form-label">Selecione o arquivo CSV:</label>
                            <input class="form-control" type="file" name="file" id="file" accept=".csv" required>
                        </div>
                        <button type="submit" class="btn btn-custom btn-lg">
                            📤 Processar Arquivo
                        </button>
                    </form>
                </div>
            </div>
        </div>

        {% with messages = get_flashed_messages() %}
            {% if messages %}
                <div class="row mt-4">
                    <div class="col-12">
                        {% for message in messages %}
                            <div class="alert alert-info alert-dismissible fade show" role="alert">
                                {{ message }}
                                <button type="button" class="btn-close" data-bs-dismiss="alert"></button>
                            </div>
                        {% endfor %}
                    </div>
                </div>
            {% endif %}
        {% endwith %}

        <!-- NOVA SEÇÃO: Cards lado a lado -->
        <div class="row mt-5">
            <!-- Card do Modelo CSV -->
            <div class="col-md-6 mb-4">
                <div class="card h-100">
                    <div class="card-body text-center">
                        <i class="fas fa-file-download fa-3x text-success mb-3"></i>
                        <h5 class="card-title">📥 Modelo CSV</h5>
                        <p class="card-text">Baixe o modelo CSV para usar no gerador de XML</p>
                        <a href="{{ url_for('download_modelo_csv') }}" class="btn btn-success">
                            <i class="fas fa-file-excel"></i> Baixar Modelo CSV
                        </a>
                    </div>
                </div>
            </div>
            
            <!-- Card do Conversor CSV -->
            <div class="col-md-6 mb-4">
                <div class="card h-100">
                    <div class="card-body text-center">
                        <i class="fas fa-exchange-alt fa-3x text-primary mb-3"></i>
                        <h5 class="card-title">🔄 Conversor CSV</h5>
                        <p class="card-text">Converta Enderecos_Totais_CO.csv para formato Power Query</p>
                        <a href="/conversor-csv" class="btn btn-primary">
                            <i class="fas fa-cogs"></i> Acessar Conversor
                        </a>
                    </div>
                </div>
            </div>
        </div>

        <div class="row mt-4">
            <div class="col-12">
                <div class="card">
                    <div class="card-header">
                        <h5>ℹ️ Informações do CSV</h5>
                    </div>
                    <div class="card-body">
                    <p><strong>Tem duas forma de gerar o XML:</strong></p>
                        <ul>
                            <li><strong>Com dois complementos:</strong> Preencher as colunas COMPLEMENTO e COMPLEMENTO2 e deixar a coluna COMPLEMENTO3 vazia</li>
                            <li><strong>Com três complementos:</strong> Preencher as colunas COMPLEMENTO, COMPLEMENTO2 e RESULTADO</li>
                        </ul>
                    <br>
                    <p><strong>O arquivo CSV deve conter as seguintes colunas:</strong></p>
                    <ul>
                        <li>COMPLEMENTO, COMPLEMENTO2, RESULTADO</li>
                        <li>LATITUDE, LONGITUDE, COD_ZONA</li>
                        <li>LOCALIDADE, LOGRADOURO, BAIRRO</li>
                        <li>MUNICIPIO, UF, COD_LOGRADOURO</li>
                        <li>ID_ENDERECO, ID_ROTEIRO, ID_LOCALIDADE</li>
                        <li>CEP, NUM_FACHADA, COD_SURVEY</li>
                        <li>QUANTIDADE_UMS, UCS_RESIDENCIAIS, UCS_COMERCIAIS</li>
                    </ul>
                    <p><strong>Separador:</strong> Ponto e vírgula (;)</p>
                    </div>
                </div>
            </div>
        </div>

        <footer class="text-center mt-5">
            <p><a href="/sobre">Sobre este sistema</a></p>
        </footer>
    </div>

    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/js/bootstrap.bundle.min.js"></script>
</body>
</html>'''
    
    # Template resultado.html
    resultado_html = '''<!DOCTYPE html>
<html lang="pt-BR">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Processamento Concluído</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/css/bootstrap.min.css" rel="stylesheet">
</head>
<body>
    <div class="container mt-5">
        <div class="row">
            <div class="col-12 text-center">
                <h1 class="text-success">✅ Processamento Concluído</h1>
                <p class="lead">{{ total_registros }} registros processados com sucesso!</p>
            </div>
        </div>

        <div class="row mt-4">
    <div class="col-12 text-center">
        <a href="{{ url_for('download_file', filename=zip_filename) }}"
           class="btn btn-primary btn-lg {% if alert_type == 'danger' %}disabled{% endif %}"
           {% if alert_type == 'danger' %}tabindex="-1" aria-disabled="true"{% endif %}>
            📥 Download do ZIP
        </a>
        <a href="/" class="btn btn-secondary btn-lg ms-2">
            🔄 Processar Outro Arquivo
        </a>
    </div>
</div>

        <div class="row mt-5">
            <div class="col-12">
                <div class="card">
                    <div class="card-header">
                        <h5>📋 Log de Processamento</h5>
                        {% if complementos %}
                            <div class="alert alert-{{ alert_type }} alert-dismissible fade show text-center" role="alert">
                            {{ complementos|safe }}
                            </div>
                        {% endif %}
                    </div>
                    <div class="card-body">
                        <pre style="max-height: 400px; overflow-y: auto;">{{ log }}</pre>
                    </div>
                </div>
            </div>
        </div>
    </div>
</body>
</html>'''
    
    # Template sobre.html
    sobre_html = '''<!DOCTYPE html>
<html lang="pt-BR">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Sobre o Sistema</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/css/bootstrap.min.css" rel="stylesheet">
</head>
<body>
    <div class="container mt-5">
        <div class="row">
            <div class="col-12">
                <h1>ℹ️ Sobre o Sistema</h1>
                
                <div class="card mt-4">
                    <div class="card-header">
                        <h5>Funcionalidades</h5>
                    </div>
                    <div class="card-body">
                        <ul>
                            <li>Processamento de arquivos CSV para geração de XML</li>
                            <li>Conversão automática de coordenadas</li>
                            <li>Mapeamento de códigos de complementos</li>
                            <li>Geração de arquivos ZIP com estrutura organizada</li>
                            <li>Interface web amigável</li>
                        </ul>
                    </div>
                </div>

                <div class="card mt-4">
                    <div class="card-header">
                        <h5>Como usar</h5>
                    </div>
                    <div class="card-body">
                        <ol>
                            <li>Faça upload de um arquivo CSV</li>
                            <li>O sistema processará automaticamente</li>
                            <li>Faça download do arquivo ZIP gerado</li>
                            <li>Os XMLs estarão organizados em pastas numeradas</li>
                        </ol>
                    </div>
                </div>

                <div class="text-center mt-4">
                    <a href="/" class="btn btn-primary">Voltar ao Início</a>
                </div>
            </div>
        </div>
    </div>
</body>
</html>'''
    
    # Template conversor_csv.html
    conversor_csv_html = '''<!DOCTYPE html>
<html lang="pt-BR">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Conversor de CSV</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/css/bootstrap.min.css" rel="stylesheet">
    <link href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/5.15.4/css/all.min.css" rel="stylesheet">
    <link rel="icon" type="image/x-icon" href="{{ url_for('static', filename='img/telemont.ico') }}">
    <style>
        .container { max-width: 800px; }
        .upload-box { border: 2px dashed #ccc; padding: 2rem; text-align: center; }
        .file-info { margin-top: 10px; font-size: 0.9em; color: #666; }
        .validation-result { margin-top: 15px; padding: 10px; border-radius: 5px; }
        .validation-success { background: #d4edda; color: #155724; border: 1px solid #c3e6cb; }
        .validation-error { background: #f8d7da; color: #721c24; border: 1px solid #f5c6cb; }
        .validation-warning { background: #fff3cd; color: #856404; border: 1px solid #ffeaa7; }
        .colunas-lista { max-height: 150px; overflow-y: auto; font-size: 0.85em; }
    </style>
</head>
<body>
    <div class="container mt-5">
        <div class="row">
            <div class="col-12 text-center">
                <img src="{{ url_for('static', filename='img/telemont.png') }}" alt="Logo Telemont" class="img-fluid p-3" style="width: 300px; display: block; margin: 0px auto;">
                <h1 class="mb-4">🔄 Conversor de CSV</h1>
                <p class="lead">Converta Enderecos_Totais_CO.csv para formato Power Query</p>
                <a href="/" class="btn btn-secondary mb-4">
                    <i class="fas fa-arrow-left"></i> Voltar ao Início
                </a>
            </div>
        </div>

        <div class="row mt-4">
            <div class="col-12">
                <div class="upload-box rounded-3">
                    <form method="POST" enctype="multipart/form-data" id="uploadForm">
                        <div class="mb-3">
                            <label for="file" class="form-label">Selecione o arquivo Enderecos_Totais_CO.csv:</label>
                            <input class="form-control" type="file" name="file" id="file" accept=".csv" required>
                            <div class="file-info" id="fileInfo">
                                📁 Tamanho máximo: 2GB | Formato: CSV com separador |
                            </div>
                            
                            <!-- Resultado da Validação -->
                            <div id="validationResult" class="validation-result" style="display: none;">
                                <div id="validationContent"></div>
                            </div>
                        </div>
                        <button type="submit" class="btn btn-primary btn-lg" id="submitBtn" disabled>
                            🔄 Converter Arquivo
                        </button>
                    </form>
                </div>
            </div>
        </div>

        {% with messages = get_flashed_messages() %}
            {% if messages %}
                <div class="row mt-4">
                    <div class="col-12">
                        {% for message in messages %}
                            <div class="alert alert-info alert-dismissible fade show" role="alert">
                                {{ message }}
                                <button type="button" class="btn-close" data-bs-dismiss="alert"></button>
                            </div>
                        {% endfor %}
                    </div>
                </div>
            {% endif %}
        {% endwith %}

        <div class="row mt-5">
            <div class="col-12">
                <div class="card">
                    <div class="card-header">
                        <h5>ℹ️ Informações do Conversor</h5>
                    </div>
                    <div class="card-body">
                        <p><strong>Este conversor realiza as seguintes operações:</strong></p>
                        <ul>
                            <li>Formata CEP e COD_LOGRADOURO</li>
                            <li>Cria CHAVE LOG para agrupamento</li>
                            <li>Processa COMPLEMENTO3 com numeração automática</li>
                            <li>Adiciona informações de roteiro</li>
                            <li>Gera validações dos dados</li>
                            <li>Formata para Power Query (UTF-8 com BOM)</li>
                        </ul>
                        
                        <p><strong>Colunas obrigatórias no CSV:</strong></p>
                        <div class="colunas-lista">
                            <ul class="list-unstyled">
                                {% for coluna in [
                                    'CELULA', 'ESTACAO_ABASTECEDORA', 'UF', 'MUNICIPIO', 'LOCALIDADE', 
                                    'COD_LOCALIDADE', 'LOCALIDADE_ABREV', 'LOGRADOURO', 'COD_LOGRADOURO', 
                                    'NUM_FACHADA', 'COMPLEMENTO', 'COMPLEMENTO2', 'COMPLEMENTO3', 'CEP', 
                                    'BAIRRO', 'COD_SURVEY', 'QUANTIDADE_UMS', 'COD_VIABILIDADE', 
                                    'TIPO_VIABILIDADE', 'TIPO_REDE', 'UCS_RESIDENCIAIS', 'UCS_COMERCIAIS', 
                                    'NOME_CDO', 'ID_ENDERECO', 'LATITUDE', 'LONGITUDE', 'TIPO_SURVEY', 
                                    'REDE_INTERNA', 'UMS_CERTIFICADAS', 'REDE_EDIF_CERT', 'DISP_COMERCIAL', 
                                    'ESTADO_CONTROLE', 'DATA_ESTADO_CONTROLE', 'ID_CELULA', 'QUANTIDADE_HCS'
                                ] %}
                                    <li><code>{{ coluna }}</code></li>
                                {% endfor %}
                            </ul>
                        </div>
                        
                        <p><strong>Arquivo de entrada:</strong> Enderecos_Totais_CO.csv (separador |)</p>
                        <p><strong>Arquivo de saída:</strong> CSV formatado para Power Query (separador ;)</p>
                        <div class="alert alert-warning">
                            <strong>⚠️ Para arquivos grandes (760MB+):</strong>
                            <ul class="mb-0">
                                <li>O processamento pode levar vários minutos</li>
                                <li>Recomendado para servidores com boa memória RAM</li>
                                <li>Será processado em partes para otimizar performance</li>
                            </ul>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    </div>

    <script>
    // Elementos da página
    const fileInput = document.getElementById('file');
    const fileInfo = document.getElementById('fileInfo');
    const submitBtn = document.getElementById('submitBtn');
    const validationResult = document.getElementById('validationResult');
    const validationContent = document.getElementById('validationContent');

    // Criar elemento de loading
    const loadingSpinner = document.createElement('div');
    loadingSpinner.id = 'loadingSpinner';
    loadingSpinner.style.cssText = `
        display: none;
        text-align: center;
        padding: 10px;
        margin: 10px 0;
    `;
    
    const spinnerHTML = `
        <div style="display: inline-block; width: 20px; height: 20px; border: 3px solid #f3f3f3; border-top: 3px solid #3498db; border-radius: 50%; animation: spin 1s linear infinite;"></div>
        <span style="margin-left: 10px; color: #3498db;">Validando arquivo...</span>
        <div style="font-size: 12px; color: #666; margin-top: 5px;" id="loadingTime">Tempo: 0s</div>
    `;
    
    loadingSpinner.innerHTML = spinnerHTML;
    
    // Adicionar estilo de animação
    const style = document.createElement('style');
    style.textContent = `
        @keyframes spin {
            0% { transform: rotate(0deg); }
            100% { transform: rotate(360deg); }
        }
    `;
    document.head.appendChild(style);
    
    // Inserir o spinner após o fileInfo
    fileInfo.parentNode.insertBefore(loadingSpinner, fileInfo.nextSibling);

    // Variáveis para o temporizador
    let validationTimer;
    let seconds = 0;

    // Função para iniciar o temporizador
    function startTimer() {
        seconds = 0;
        const timeElement = document.getElementById('loadingTime');
        timeElement.textContent = `Tempo: 0s`;
        
        validationTimer = setInterval(() => {
            seconds++;
            timeElement.textContent = `Tempo: ${seconds}s`;
        }, 1000);
        
        loadingSpinner.style.display = 'block';
    }

    // Função para parar o temporizador
    function stopTimer() {
        if (validationTimer) {
            clearInterval(validationTimer);
            validationTimer = null;
        }
        loadingSpinner.style.display = 'none';
    }

    // Função para mostrar resultado da validação
    function showValidationResult(result) {
        validationResult.style.display = 'block';
        
        if (result.valido) {
            validationResult.className = 'validation-result validation-success';
            let html = `<strong>✅ Arquivo válido!</strong><br>`;
            html += `${result.total_colunas} - Colunas encontradas<br>`;
            if (result.colunas_extras && result.colunas_extras.length > 0) {
                html += `<small>Colunas extras: ${result.colunas_extras.join(', ')}</small>`;
            }
            validationContent.innerHTML = html;
            submitBtn.disabled = false;
        } else {
            validationResult.className = 'validation-result validation-error';
            let html = `<strong>❌ Arquivo inválido!</strong><br>`;
            if (result.erro) {
                html += `Erro: ${result.erro}`;
            } else {
                html += `Colunas faltantes: ${result.colunas_faltantes.join(', ')}<br>`;
                html += `Total de colunas no arquivo: ${result.total_colunas}`;
            }
            validationContent.innerHTML = html;
            submitBtn.disabled = true;
        }
    }

    // Função para validar tamanho do arquivo
    function validarTamanhoArquivo(file) {
        const fileSizeMB = (file.size / (1024 * 1024)).toFixed(2);
        const fileSizeGB = (file.size / (1024 * 1024 * 1024)).toFixed(2);
        
        // Definir limites de tamanho
        const LIMITE_PEQUENO = 5; // 5MB
        const LIMITE_GRANDE = 100; // 100MB
        const LIMITE_MAXIMO = 2 * 1024; // 2GB
        
        if (file.size > LIMITE_MAXIMO * 1024 * 1024) {
            return {
                valido: false,
                tipo: 'tamanho',
                mensagem: `❌ Arquivo muito grande! Tamanho: ${fileSizeGB} GB (Máximo: 2GB)`
            };
        } else if (file.size > LIMITE_GRANDE * 1024 * 1024) {
            return {
                valido: true,
                tipo: 'tamanho',
                mensagem: `⚠️ Arquivo grande: ${fileSizeMB} MB. A conversão pode demorar.`
            };
        } else if (file.size < LIMITE_PEQUENO * 1024 * 1024) {
            return {
                valido: true,
                tipo: 'tamanho',
                mensagem: `📄 Arquivo pequeno: ${fileSizeMB} MB. Processamento rápido.`
            };
        } else {
            return {
                valido: true,
                tipo: 'tamanho',
                mensagem: `📊 Arquivo de tamanho moderado: ${fileSizeMB} MB.`
            };
        }
    }

    // Função para validar estrutura do arquivo via AJAX
    function validarEstruturaArquivo(file) {
        const formData = new FormData();
        formData.append('file', file);

        return fetch('/validar-csv', {
            method: 'POST',
            body: formData
        })
        .then(response => response.json())
        .then(data => {
            return {
                valido: data.valido,
                tipo: 'estrutura',
                dados: data
            };
        })
        .catch(error => {
            console.error('Erro na validação:', error);
            return {
                valido: false,
                tipo: 'estrutura',
                dados: {
                    valido: false,
                    erro: 'Erro ao validar estrutura do arquivo'
                }
            };
        });
    }

    // Função para atualizar informações do arquivo
    function atualizarFileInfo(mensagem, classe = '') {
        if (classe) {
            fileInfo.innerHTML = `<span class="${classe}">${mensagem}</span>`;
        } else {
            fileInfo.innerHTML = mensagem;
        }
    }

    // Event listener para mudança de arquivo
    fileInput.addEventListener('change', async function(e) {
        validationResult.style.display = 'none';
        submitBtn.disabled = true;
        stopTimer(); // Parar qualquer timer anterior
        
        if (this.files && this.files[0]) {
            const file = this.files[0];
            
            // Iniciar o temporizador e mostrar spinner
            startTimer();
            
            // Primeira validação: Estrutura do CSV
            atualizarFileInfo('🔍 Validando estrutura do arquivo CSV...', 'text-warning');
            
            try {
                // Validar estrutura (colunas)
                const resultadoEstrutura = await validarEstruturaArquivo(file);
                
                // Parar o temporizador após a validação
                stopTimer();
                
                if (!resultadoEstrutura.valido) {
                    // Se a estrutura for inválida, mostrar erro e parar aqui
                    showValidationResult(resultadoEstrutura.dados);
                    atualizarFileInfo(`📁 ${file.name} - Estrutura inválida`, 'text-danger');
                    return;
                }
                
                // Segunda validação: Tamanho do arquivo (só se a estrutura for válida)
                const resultadoTamanho = validarTamanhoArquivo(file);
                
                // Mostrar resultado da validação de estrutura
                showValidationResult(resultadoEstrutura.dados);
                
                // Atualizar informações do arquivo com resultado do tamanho
                const infoBase = `📁 Arquivo: ${file.name}`;
                if (resultadoTamanho.valido) {
                    atualizarFileInfo(`${infoBase} | ${resultadoTamanho.mensagem}`, 
                                   resultadoTamanho.mensagem.includes('⚠️') ? 'text-warning' : 'text-success');
                } else {
                    atualizarFileInfo(`${infoBase} | ${resultadoTamanho.mensagem}`, 'text-danger');
                    submitBtn.disabled = true;
                }
                
            } catch (error) {
                // Parar o temporizador em caso de erro
                stopTimer();
                
                console.error('Erro no processo de validação:', error);
                atualizarFileInfo('❌ Erro durante a validação do arquivo', 'text-danger');
                showValidationResult({
                    valido: false,
                    erro: 'Falha no processo de validação'
                });
            }
            
        } else {
            atualizarFileInfo('📁 Tamanho máximo: 2GB | Formato: CSV com separador |');
            submitBtn.disabled = true;
        }
    });

    // Prevenir envio se o botão estiver desabilitado
    document.getElementById('uploadForm').addEventListener('submit', function(e) {
        if (submitBtn.disabled) {
            e.preventDefault();
            alert('Por favor, selecione um arquivo CSV válido antes de converter.');
        }
    });

    // Parar o timer se o usuário mudar de página ou fechar
    window.addEventListener('beforeunload', function() {
        stopTimer();
    });
</script>

    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/js/bootstrap.bundle.min.js"></script>
</body>
</html>'''
    
    # Template resultado_conversor.html
    resultado_conversor_html = '''<!DOCTYPE html>
<html lang="pt-BR">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Conversão Concluída</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/css/bootstrap.min.css" rel="stylesheet">
</head>
<body>
    <div class="container mt-5">
        <div class="row">
            <div class="col-12 text-center">
                <h1 class="text-success">✅ Conversão Concluída</h1>
                <p class="lead">{{ total_registros }} registros processados com sucesso!</p>
            </div>
        </div>

        <div class="row mt-4">
            <div class="col-12 text-center">
                <a href="{{ url_for('download_convertido', filename=zip_filename) }}"
                   class="btn btn-primary btn-lg">
                    📥 Download do CSV Convertido
                </a>
                <a href="/conversor-csv" class="btn btn-secondary btn-lg ms-2">
                    🔄 Converter Outro Arquivo
                </a>
                <a href="/" class="btn btn-success btn-lg ms-2">
                    🏠 Página Inicial
                </a>
            </div>
        </div>

        <div class="row mt-5">
            <div class="col-12">
                <div class="card">
                    <div class="card-header">
                        <h5>📋 Informações do Arquivo</h5>
                    </div>
                    <div class="card-body">
                        <p><strong>Arquivo gerado:</strong> {{ zip_filename }}</p>
                        <p><strong>Configuração para Power Query:</strong></p>
                        <ul>
                            <li>Encoding: UTF-8 com BOM</li>
                            <li>Separador: Ponto e vírgula (;)</li>
                            <li>Delimitador: Aspas (")</li>
                            <li>Origem: CSV</li>
                        </ul>
                    </div>
                </div>
            </div>
        </div>
    </div>
</body>
</html>'''

    # Template progresso.html (NOVO)
    progresso_html = '''<!DOCTYPE html>
<html lang="pt-BR">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Processando Arquivo</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/css/bootstrap.min.css" rel="stylesheet">
    <link href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/5.15.4/css/all.min.css" rel="stylesheet">
    <link rel="icon" type="image/x-icon" href="{{ url_for('static', filename='img/telemont.ico') }}">
    <style>
        .container { max-width: 800px; }
        .progress { height: 25px; margin: 20px 0; }
        .progress-bar { transition: width 0.3s ease; }
        .log-container { 
            max-height: 300px; 
            overflow-y: auto; 
            background: #f8f9fa; 
            border: 1px solid #dee2e6; 
            border-radius: 5px; 
            padding: 15px;
            font-family: 'Courier New', monospace;
            font-size: 0.9em;
        }
        .log-entry { margin-bottom: 5px; white-space: pre-wrap; }
        .spinner-border { margin-right: 10px; }
        .connection-status { 
            position: fixed; 
            top: 10px; 
            right: 10px; 
            padding: 5px 10px; 
            border-radius: 15px; 
            font-size: 0.8em;
        }
        .connected { background: #d4edda; color: #155724; }
        .disconnected { background: #f8d7da; color: #721c24; }
    </style>
</head>
<body>
    <div class="connection-status" id="connectionStatus">
        <i class="fas fa-circle"></i> <span id="statusText">Conectando...</span>
    </div>

    <div class="container mt-5">
        <div class="row">
            <div class="col-12 text-center">
                <img src="{{ url_for('static', filename='img/telemont.png') }}" alt="Logo Telemont" class="img-fluid p-3" style="width: 300px; display: block; margin: 0px auto;">
                <h1 class="mb-4">🔄 Processando Arquivo</h1>
                <p class="lead" id="statusMessage">Iniciando processamento...</p>
            </div>
        </div>
        <div class="row mt-4">
            <div class="col-12 text-center">
                <div class="alert alert-info">
                    <i class="fas fa-info-circle"></i>
                    <strong>Importante:</strong> Não feche esta página durante o processamento.
                    Para arquivos grandes, isso pode levar vários minutos.
                </div>
                <div id="actionButtons" style="display: none;">
                    <a href="/conversor-csv" class="btn btn-secondary">
                        <i class="fas fa-arrow-left"></i> Voltar ao Conversor
                    </a>
                    <a href="/conversor-result" class="btn btn-primary ms-2" id="resultButton">
                        <i class="fas fa-download"></i> Ir para Download
                    </a>
                </div>
            </div>
        </div>
        <div class="row mt-4">
            <div class="col-12">
                <div class="card">
                    <div class="card-header">
                        <h5>
                            <i class="fas fa-sync-alt fa-spin" id="statusIcon"></i>
                            Progresso do Processamento
                        </h5>
                    </div>
                    <div class="card-body">
                        <!-- Barra de Progresso -->
                        <div class="progress">
                            <div class="progress-bar progress-bar-striped progress-bar-animated" 
                                 role="progressbar" 
                                 id="progressBar"
                                 style="width: 0%">
                                <span id="progressText">0%</span>
                            </div>
                        </div>

                        <!-- Contadores -->
                        <div class="row text-center mb-3">
                            <div class="col-md-4">
                                <strong>Progresso:</strong>
                                <div id="progressPercent">0%</div>
                            </div>
                            <div class="col-md-4">
                                <strong>Linhas Processadas:</strong>
                                <div id="currentCount">0</div>
                            </div>
                            <div class="col-md-4">
                                <strong>Total de Linhas:</strong>
                                <div id="totalCount">0</div>
                            </div>
                        </div>

                        <!-- Logs em Tempo Real -->
                        <h6>Logs do Processamento:</h6>
                        <div class="log-container" id="logContainer">
                            <div class="log-entry">🕒 Iniciando monitoramento...</div>
                        </div>
                    </div>
                </div>
            </div>
        </div>

        
    </div>

    <script>
        const progressBar = document.getElementById('progressBar');
        const progressText = document.getElementById('progressText');
        const progressPercent = document.getElementById('progressPercent');
        const currentCount = document.getElementById('currentCount');
        const totalCount = document.getElementById('totalCount');
        const logContainer = document.getElementById('logContainer');
        const statusMessage = document.getElementById('statusMessage');
        const statusIcon = document.getElementById('statusIcon');
        const connectionStatus = document.getElementById('connectionStatus');
        const statusText = document.getElementById('statusText');
        const actionButtons = document.getElementById('actionButtons');
        const resultButton = document.getElementById('resultButton');

        let reconnectAttempts = 0;
        const maxReconnectAttempts = 5;

        // Função para adicionar log
        function addLog(message) {
            const logEntry = document.createElement('div');
            logEntry.className = 'log-entry';
            logEntry.innerHTML = message;
            logContainer.appendChild(logEntry);
            logContainer.scrollTop = logContainer.scrollHeight;
            
            // Limitar número de logs
            const logs = logContainer.getElementsByClassName('log-entry');
            if (logs.length > 100) {
                logContainer.removeChild(logs[0]);
            }
        }

        // Função para atualizar status da conexão
        function updateConnectionStatus(connected) {
            if (connected) {
                connectionStatus.className = 'connection-status connected';
                statusText.innerHTML = '<i class="fas fa-wifi"></i> Conectado';
                reconnectAttempts = 0;
            } else {
                connectionStatus.className = 'connection-status disconnected';
                statusText.innerHTML = '<i class="fas fa-wifi-slash"></i> Desconectado';
            }
        }

        // Função para conectar ao SSE
        function connectSSE() {
            updateConnectionStatus(false);
            
            const eventSource = new EventSource('/progress');

            eventSource.onopen = function() {
                console.log('Conexão SSE aberta');
                updateConnectionStatus(true);
                addLog('✅ Conectado ao servidor');
            };

            eventSource.onmessage = function(event) {
                try {
                    const data = JSON.parse(event.data);
                    
                    // Atualizar barra de progresso
                    if (data.progress !== undefined) {
                        progressBar.style.width = data.progress + '%';
                        progressText.textContent = Math.round(data.progress) + '%';
                        progressPercent.textContent = Math.round(data.progress) + '%';
                    }
                    
                    // Atualizar contadores
                    if (data.current !== undefined) {
                        currentCount.textContent = data.current.toLocaleString();
                    }
                    if (data.total !== undefined) {
                        totalCount.textContent = data.total.toLocaleString();
                    }
                    
                    // Atualizar mensagem de status
                    if (data.message) {
                        statusMessage.textContent = data.message;
                        if (data.message !== 'Aguardando...' && data.message !== 'Conectado...') {
                            addLog(data.message);
                        }
                    }
                    
                    // Verificar status
                    if (data.status === 'completed') {
                        statusIcon.className = 'fas fa-check-circle text-success';
                        statusMessage.innerHTML = '<span class="text-success">✅ Processamento concluído com sucesso!</span>';
                        actionButtons.style.display = 'block';
                        resultButton.style.display = 'inline-block';
                        eventSource.close();
                        
                    } else if (data.status === 'error') {
                        statusIcon.className = 'fas fa-exclamation-triangle text-danger';
                        statusMessage.innerHTML = '<span class="text-danger">❌ Erro no processamento!</span>';
                        actionButtons.style.display = 'block';
                        resultButton.style.display = 'none';
                        eventSource.close();
                    }
                    
                } catch (e) {
                    console.error('Erro ao processar mensagem:', e);
                }
            };

            eventSource.onerror = function(event) {
                console.error('Erro na conexão SSE:', event);
                updateConnectionStatus(false);
                eventSource.close();
                
                // Tentar reconectar
                if (reconnectAttempts < maxReconnectAttempts) {
                    reconnectAttempts++;
                    addLog(`🔁 Tentativa de reconexão ${reconnectAttempts}/${maxReconnectAttempts}...`);
                    setTimeout(connectSSE, 2000);
                } else {
                    addLog('❌ Falha na conexão. Por favor, recarregue a página.');
                    statusIcon.className = 'fas fa-exclamation-triangle text-danger';
                    actionButtons.style.display = 'block';
                }
            };

            return eventSource;
        }

        // Iniciar conexão quando a página carregar
        let sseConnection = connectSSE();

        // Tentar reconectar se a página ficar visível novamente
        document.addEventListener('visibilitychange', function() {
            if (!document.hidden && (sseConnection.readyState === EventSource.CLOSED)) {
                addLog('🔄 Reconectando...');
                sseConnection = connectSSE();
            }
        });

        // Limpar recursos quando a página for fechada
        window.addEventListener('beforeunload', function() {
            if (sseConnection) {
                sseConnection.close();
            }
        });
    </script>

    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/js/bootstrap.bundle.min.js"></script>
</body>
</html>'''

    # Escrever os templates
    with open(os.path.join(templates_dir, 'index.html'), 'w', encoding='utf-8') as f:
        f.write(index_html)
    
    with open(os.path.join(templates_dir, 'resultado.html'), 'w', encoding='utf-8') as f:
        f.write(resultado_html)
    
    with open(os.path.join(templates_dir, 'sobre.html'), 'w', encoding='utf-8') as f:
        f.write(sobre_html)
    
    with open(os.path.join(templates_dir, 'conversor_csv.html'), 'w', encoding='utf-8') as f:
        f.write(conversor_csv_html)
    
    with open(os.path.join(templates_dir, 'resultado_conversor.html'), 'w', encoding='utf-8') as f:
        f.write(resultado_conversor_html)
    
    with open(os.path.join(templates_dir, 'progresso.html'), 'w', encoding='utf-8') as f:
        f.write(progresso_html)

if __name__ == '__main__':
    # Criar diretório de templates se não existir
    templates_dir = os.path.join(os.path.dirname(__file__), 'templates')
    if not os.path.exists(templates_dir):
        os.makedirs(templates_dir)
    
    # Criar templates básicos
    criar_templates()
    
    # Limpar arquivos antigos ao iniciar
    limpar_arquivos_antigos()
    
    app.run(debug=True, host='0.0.0.0', port=5000)