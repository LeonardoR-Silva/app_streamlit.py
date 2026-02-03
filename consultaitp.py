import streamlit as st
import pandas as pd
import zipfile
import requests
from io import BytesIO, StringIO
from datetime import datetime
import tempfile
import os

# Configuração da página
st.set_page_config(
    page_title="Consulta ITP 2025",
    page_icon="🔍",
    layout="centered",
    initial_sidebar_state="collapsed"
)

# CSS customizado
st.markdown("""
    <style>
        .main {
            max-width: 700px;
            margin: 0 auto;
        }
        .stButton > button {
            width: 100%;
            height: 45px;
            font-size: 16px;
            font-weight: 600;
        }
        .success-box {
            background-color: #e8f5e9;
            border: 1px solid #4caf50;
            border-radius: 8px;
            padding: 16px;
            margin: 16px 0;
            color: #2e7d32;
        }
        .error-box {
            background-color: #ffebee;
            border: 1px solid #f44336;
            border-radius: 8px;
            padding: 16px;
            margin: 16px 0;
            color: #c62828;
        }
        .info-box {
            background-color: #e3f2fd;
            border: 1px solid #2196f3;
            border-radius: 8px;
            padding: 16px;
            margin: 16px 0;
            color: #1565c0;
        }
    </style>
""", unsafe_allow_html=True)

# URLs dos dados ITP
URL_2025 = 'https://radardatransparencia.atricon.org.br/dados/dados_pntp_2025.zip'
URL_2024 = 'https://radardatransparencia.atricon.org.br/dados/dados_pntp_2024.zip'

# Mapa de estados
ESTADOS_MAP = {
    'AC': 'Acre', 'AL': 'Alagoas', 'AP': 'Amapá', 'AM': 'Amazonas',
    'BA': 'Bahia', 'CE': 'Ceará', 'DF': 'Distrito Federal', 'ES': 'Espírito Santo',
    'GO': 'Goiás', 'MA': 'Maranhão', 'MT': 'Mato Grosso', 'MS': 'Mato Grosso do Sul',
    'MG': 'Minas Gerais', 'PA': 'Pará', 'PB': 'Paraíba', 'PR': 'Paraná',
    'PE': 'Pernambuco', 'PI': 'Piauí', 'RJ': 'Rio de Janeiro', 'RN': 'Rio Grande do Norte',
    'RS': 'Rio Grande do Sul', 'RO': 'Rondônia', 'RR': 'Roraima', 'SC': 'Santa Catarina',
    'SP': 'São Paulo', 'SE': 'Sergipe', 'TO': 'Tocantins'
}


@st.cache_resource
def load_itp_data():
    """Carrega dados ITP em cache"""
    try:
        with st.spinner('⏳ Carregando dados ITP 2025/2024...'):
            # Baixar e processar 2025
            st.write("📥 Baixando dados 2025...")
            response_2025 = requests.get(URL_2025, timeout=60)
            response_2025.raise_for_status()
            
            st.write("📥 Baixando dados 2024...")
            response_2024 = requests.get(URL_2024, timeout=60)
            response_2024.raise_for_status()
            
            # Extrair CSVs
            st.write("📂 Extraindo arquivos...")
            zip_2025 = zipfile.ZipFile(BytesIO(response_2025.content))
            zip_2024 = zipfile.ZipFile(BytesIO(response_2024.content))
            
            csv_2025 = None
            csv_2024 = None
            
            for file_info in zip_2025.filelist:
                if 'respostas_avaliacoes_pntp_2025.csv' in file_info.filename:
                    csv_2025 = zip_2025.read(file_info).decode('utf-8')
                    st.write(f"✓ Encontrado: {file_info.filename}")
                    break
            
            for file_info in zip_2024.filelist:
                if 'respostas_avaliacoes_pntp_2024.csv' in file_info.filename:
                    csv_2024 = zip_2024.read(file_info).decode('utf-8')
                    st.write(f"✓ Encontrado: {file_info.filename}")
                    break
            
            # Carregar DataFrames
            st.write("📊 Processando dados...")
            df_2025 = pd.read_csv(StringIO(csv_2025), sep=";")
            df_2024 = pd.read_csv(StringIO(csv_2024), sep=";")
            
            # Estruturar dados por estado
            dados_estruturados = {}
            
            for year, df in [(2025, df_2025), (2024, df_2024)]:
                if 'estado' in df.columns:
                    for estado in df['estado'].unique():
                        if pd.notna(estado):
                            if estado not in dados_estruturados:
                                dados_estruturados[estado] = {}
                            
                            entidades = df[df['estado'] == estado]
                            dados_estruturados[estado][year] = entidades
            
            st.success('✅ Dados carregados com sucesso!')
            return dados_estruturados, df_2025, df_2024
    
    except Exception as e:
        st.error(f'❌ Erro ao carregar dados: {str(e)}')
        return None, None, None


def gerar_arquivo_excel(df, nome_arquivo):
    """Gera arquivo Excel em memória"""
    try:
        output = BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Dados ITP', index=False)
        
        output.seek(0)
        return output
    except Exception as e:
        st.error(f'Erro ao gerar Excel: {str(e)}')
        return None


# Interface principal
col1, col2, col3 = st.columns([1, 2, 1])
with col2:
    st.title("🔍 Consulta ITP 2025")
st.markdown("---")
st.write("**Download de questionários por entidade**")
st.markdown("")

# Carregar dados
dados_estruturados, df_2025, df_2024 = load_itp_data()

if dados_estruturados is None:
    st.error("❌ Não foi possível carregar os dados. Verifique sua conexão de internet.")
    st.stop()

# Etapa 1: Selecionar Estado
st.subheader("1️⃣ Selecionar Estado")
estados_disponiveis = sorted(dados_estruturados.keys())
sigla_selecionada = st.selectbox(
    "Escolha um estado:",
    options=[""] + estados_disponiveis,
    format_func=lambda x: f"{ESTADOS_MAP.get(x, x)} ({x})" if x else "-- Selecione um estado --"
)

if not sigla_selecionada:
    st.info("👆 Selecione um estado para continuar")
    st.stop()

# Etapa 2: Buscar Entidade
st.subheader("2️⃣ Buscar Entidade")
entidades_2025 = dados_estruturados[sigla_selecionada].get(2025, pd.DataFrame())
entidades_2024 = dados_estruturados[sigla_selecionada].get(2024, pd.DataFrame())

# Combinar e deduplicate
todas_entidades = set()
if not entidades_2025.empty and 'entidade' in entidades_2025.columns:
    todas_entidades.update(entidades_2025['entidade'].dropna().unique())
if not entidades_2024.empty and 'entidade' in entidades_2024.columns:
    todas_entidades.update(entidades_2024['entidade'].dropna().unique())

todas_entidades = sorted(list(todas_entidades))

if not todas_entidades:
    st.error(f"❌ Nenhuma entidade encontrada para {ESTADOS_MAP.get(sigla_selecionada, sigla_selecionada)}")
    st.stop()

# Campo de busca
termo_busca = st.text_input(
    "Digite para filtrar entidades:",
    placeholder="Ex: Prefeitura, Câmara, Assembleia...",
    key="search"
)

# Filtrar entidades
entidades_filtradas = [e for e in todas_entidades 
                      if termo_busca.lower() in e.lower()]

if not entidades_filtradas and termo_busca:
    st.warning(f"⚠️ Nenhuma entidade encontrada para '{termo_busca}'")
    st.stop()

# Etapa 3: Selecionar Entidade
st.subheader("3️⃣ Selecionar Entidade")
entidade_selecionada = st.selectbox(
    "Escolha uma entidade:",
    options=[""] + entidades_filtradas,
    format_func=lambda x: x if x else "-- Selecione uma entidade --",
    key="entity_select"
)

if not entidade_selecionada:
    st.info("👆 Selecione uma entidade para continuar")
    st.stop()

# Mostrar entidade selecionada
st.markdown(f"""
<div class="info-box">
    <strong>✓ Entidade selecionada:</strong><br>
    {entidade_selecionada}
</div>
""", unsafe_allow_html=True)

# Botões de ação
col1, col2 = st.columns(2)

with col1:
    btn_gerar = st.button("📥 Gerar Downloads", use_container_width=True, type="primary")

with col2:
    btn_limpar = st.button("🔄 Limpar", use_container_width=True)

if btn_limpar:
    st.session_state.search = ""
    st.session_state.entity_select = ""
    st.rerun()

# Gerar downloads
if btn_gerar:
    st.markdown("---")
    st.subheader("📊 Arquivos Disponíveis")
    
    with st.spinner('⏳ Processando solicitação...'):
        try:
            # Filtrar dados para 2025
            if not entidades_2025.empty:
                df_2025_filtrado = entidades_2025[
                    entidades_2025['entidade'] == entidade_selecionada
                ].copy()
            else:
                df_2025_filtrado = pd.DataFrame()
            
            # Filtrar dados para 2024
            if not entidades_2024.empty:
                df_2024_filtrado = entidades_2024[
                    entidades_2024['entidade'] == entidade_selecionada
                ].copy()
            else:
                df_2024_filtrado = pd.DataFrame()
            
            if df_2025_filtrado.empty and df_2024_filtrado.empty:
                st.error("❌ Nenhum dado encontrado para essa entidade")
                st.stop()
            
            # Gerar arquivos
            arquivos = {}
            
            if not df_2025_filtrado.empty:
                excel_2025 = gerar_arquivo_excel(
                    df_2025_filtrado,
                    f"questionario_itp_2025_{entidade_selecionada}"
                )
                if excel_2025:
                    arquivos['2025'] = {
                        'data': excel_2025,
                        'nome': f"questionario_itp_2025_{entidade_selecionada}.xlsx",
                        'linhas': len(df_2025_filtrado),
                        'colunas': len(df_2025_filtrado.columns)
                    }
            
            if not df_2024_filtrado.empty:
                excel_2024 = gerar_arquivo_excel(
                    df_2024_filtrado,
                    f"questionario_itp_2024_{entidade_selecionada}"
                )
                if excel_2024:
                    arquivos['2024'] = {
                        'data': excel_2024,
                        'nome': f"questionario_itp_2024_{entidade_selecionada}.xlsx",
                        'linhas': len(df_2024_filtrado),
                        'colunas': len(df_2024_filtrado.columns)
                    }
            
            # Exibir arquivos
            if arquivos:
                st.success(f"✅ Arquivos gerados para: **{entidade_selecionada}**")
                
                for ano, info in sorted(arquivos.items(), reverse=True):
                    col1, col2 = st.columns([3, 1])
                    
                    with col1:
                        st.write(f"**📄 ITP {ano}**")
                        st.caption(f"📊 {info['linhas']} linhas • {info['colunas']} colunas")
                    
                    with col2:
                        st.download_button(
                            label="⬇️ Download",
                            data=info['data'],
                            file_name=info['nome'],
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            use_container_width=True
                        )
                
                # Resumo
                st.markdown("---")
                st.markdown(f"""
                <div class="success-box">
                    <strong>✓ Processamento concluído!</strong><br>
                    Clique no botão para fazer download do arquivo
                </div>
                """, unsafe_allow_html=True)
        
        except Exception as e:
            st.error(f"❌ Erro ao gerar arquivo: {str(e)}")
            import traceback
            st.write(traceback.format_exc())

# Rodapé
st.markdown("---")
col1, col2, col3 = st.columns(3)
with col1:
    st.caption(f"🔄 Última atualização: {datetime.now().strftime('%d/%m/%Y às %H:%M')}")
with col2:
    st.caption("📡 Dados do Radar da Transparência")
with col3:
    st.caption("🚀 Powered by Streamlit")
