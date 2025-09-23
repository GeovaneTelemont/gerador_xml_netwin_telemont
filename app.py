import pandas as pd
import xml.etree.ElementTree as ET
import os
import zipfile
from datetime import datetime
from flask import Flask, request, render_template, send_file, flash, redirect, url_for, session
from werkzeug.utils import secure_filename
import tempfile
import shutil
import time

app = Flask(__name__)
app.secret_key = 'sua_chave_secreta_aqui'  # Altere para uma chave segura
app.config['UPLOAD_FOLDER'] = tempfile.mkdtemp()
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max file size

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
LOG_COMPLEMENTOS = ""
ERRO_COMPLEMENTO3 = False
ERRO_COMPLEMENTO2 = False

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

def criar_xml_edificio_com_dois_complementos(dados_csv, numero_pasta):
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


def criar_xml_edificio_com_tres_complementos(dados_csv, numero_pasta, complemento_vazio):
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
    print("complemento vazio xml: ", complemento_vazio)
    if complemento_vazio == False:
        complemento3 = dados_csv['RESULTADO'] if 'RESULTADO' in dados_csv else ''
        if not pd.isna(complemento3) and str(complemento3).strip() != '':
            codigo_complemento3 = obter_codigo_complemento(complemento3)
            argumento3 = extrair_numero_argumento(complemento3)
            ET.SubElement(endereco, 'id_complemento3').text = codigo_complemento3
            ET.SubElement(endereco, 'argumento3').text = argumento3
            print("complemento 3 adicionado")

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
                print(f"Arquivo lido com encoding: {encoding}")
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
        
        print("coluna complemento 2 vazia:", coluna_complemento_2_vazia)
        nome_pasta = f'moradia{i}'
        caminho_pasta = os.path.join(diretorio_principal, nome_pasta)
        os.makedirs(caminho_pasta, exist_ok=True)
        pastas_criadas.append(caminho_pasta)

        comp1 = linha['COMPLEMENTO'] if 'COMPLEMENTO' in linha else ''
        comp2 = linha['COMPLEMENTO2'] if 'COMPLEMENTO2' in linha else ''
        resultado = linha['RESULTADO'] if 'RESULTADO' in linha else ''

        xml_content = criar_xml_edificio_com_tres_complementos(linha, i, coluna_complemento_2_vazia)

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
            log_processamento.append(f'  COMP1("{comp1}" → código:{codigo1} argumento:"{arg1}")')
            log_processamento.append(f'  COMP2("{comp2}" → código:{codigo2} argumento:"{arg2}")')
            
            if coluna_complemento_2_vazia:
                pass
            else:
                codigo3 = obter_codigo_complemento(resultado)
                arg3 = extrair_numero_argumento(resultado)
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
                #alert_type = "danger" if ERRO_COMPLEMENTO3 or ERRO_COMPLEMENTO2 else "info"
                
                #alert_type = "danger" if ERRO_COMPLEMENTO2 or ERRO_COMPLEMENTO3 else "info"
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

# Templates HTML
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
    <style>
        .container { max-width: 800px; }
        .upload-box { border: 2px dashed #ccc; padding: 2rem; text-align: center; }
        .btn-custom { background-color: #0d6efd; color: white; }
    </style>
</head>
<body>
    <div class="container mt-5">
        <div class="row">
            <div class="col-12 text-center">
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

        <div class="row mt-5">
            <div class="col-12">
                <div class="card">
                    <div class="card-header">
                        <h5>ℹ️ Informações do CSV</h5>
                    </div>
                    <div class="card-body">
                        <p>O arquivo CSV deve conter as seguintes colunas:</p>
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
    
    # Escrever os templates
    with open(os.path.join(templates_dir, 'index.html'), 'w', encoding='utf-8') as f:
        f.write(index_html)
    
    with open(os.path.join(templates_dir, 'resultado.html'), 'w', encoding='utf-8') as f:
        f.write(resultado_html)
    
    with open(os.path.join(templates_dir, 'sobre.html'), 'w', encoding='utf-8') as f:
        f.write(sobre_html)

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