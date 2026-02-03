import streamlit as st
import pandas as pd
import zipfile
import requests
from io import BytesIO, StringIO
from datetime import datetime
import warnings
import os

warnings.filterwarnings('ignore')

# Configuração da página
st.set_page_config(
    page_title="Consulta ITP 2025",
    page_icon="🔍",
    layout="centered",
    initial_sidebar_state="collapsed"
)

# URLs dos dados ITP
URL_2025 = 'https://radardatransparencia.atricon.org.br/dados/dados_pntp_2025.zip'
URL_2024 = 'https://radardatransparencia.atricon.org.br/dados/dados_pntp_2024.zip'

# Paths de cache
CACHE_DIR = '.cache_itp'
CACHE_2025 = os.path.join(CACHE_DIR, 'itp_2025.parquet')
CACHE_2024 = os.path.join(CACHE_DIR, 'itp_2024.parquet')

os.makedirs(CACHE_DIR, exist_ok=True)

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

# CSS customizado
st.markdown("""
    <style>
        .main { max-width: 700px; margin: 0 auto; }
        .success-box {
            background-color: #e8f5e9;
            border: 1px solid #4caf50;
            border-radius: 8px;
            padding: 16px;
            margin: 16px 0;
            color: #2e7d32;
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


@st.cache_resource(ttl=86400)
def load_itp_data():
    """
    Carrega dados ITP com cache em Parquet
    TTL: 24 horas (atualiza automaticamente)
    """
    try:
        # Verificar se cache existe
        if os.path.exists(CACHE_2025) and os.path.exists(CACHE_2024):
            st.info("⚡ Carregando dados do cache local...")
            df_2025 = pd.read_parquet(CACHE_2025)
            df_2024 = pd.read_parquet(CACHE_2024)
            st.success('✅ Cache carregado!')
            return df_2025, df_2024, True
        
        # Cache não existe, baixar dados
        st.info("⏳ Primeira execução. Baixando e processando dados (5-10 minutos)...")
        
        # Baixar 2025
        st.write("📥 Baixando ITP 2025 (pode demorar)...")
        response_2025 = requests.get(URL_2025, timeout=600)
        response_2025.raise_for_status()
        st.write("✅ ITP 2025 baixado")
        
        # Baixar 2024
        st.write("📥 Baixando ITP 2024 (pode demorar)...")
        response_2024 = requests.get(URL_2024, timeout=600)
        response_2024.raise_for_status()
        st.write("✅ ITP 2024 baixado")
        
        # Extrair e processar 2025
        st.write("📂 Extraindo ITP 2025...")
        zip_2025 = zipfile.ZipFile(BytesIO(response_2025.content))
        csv_2025 = None
        for file_info in zip_2025.filelist:
            if 'respostas_avaliacoes_pntp_2025.csv' in file_info.filename:
                csv_2025 = zip_2025.read(file_info).decode('utf-8')
                break
        
        # Extrair e processar 2024
        st.write("📂 Extraindo ITP 2024...")
        zip_2024 = zipfile.ZipFile(BytesIO(response_2024.content))
        csv_2024 = None
        for file_info in zip_2024.filelist:
            if 'respostas_avaliacoes_pntp_2024.csv' in file_info.filename:
                csv_2024 = zip_2024.read(file_info).decode('utf-8')
                break
        
        if not csv_2025 or not csv_2024:
            st.error("❌ Arquivos CSV não encontrados")
            return None, None, False
        
        # Carregar com tipos otimizados
        st.write("📊 Processando dados (otimizando tipos)...")
        
        dtype_optimize = {
            'estado': 'category',
            'entidade': 'string',
        }
        
        df_2025 = pd.read_csv(
            StringIO(csv_2025), 
            sep=";",
            dtype=dtype_optimize,
            low_memory=False
        )
        
        df_2024 = pd.read_csv(
            StringIO(csv_2024), 
            sep=";",
            dtype=dtype_optimize,
            low_memory=False
        )
        
        # Salvar cache em Parquet (muito mais compacto)
        st.write("💾 Salvando cache em Parquet...")
        df_2025.to_parquet(CACHE_2025, compression='snappy')
        df_2024.to_parquet(CACHE_2024, compression='snappy')
        st.write("✅ Cache salvo")
        
        # Mostrar tamanho
        size_2025 = os.path.getsize(CACHE_2025) / (1024*1024)
        size_2024 = os.path.getsize(CACHE_2024) / (1024*1024)
        st.success(f'✅ Dados processados! Cache: {size_2025:.1f}MB + {size_2024:.1f}MB')
        
        return df_2025, df_2024, True
    
    except requests.exceptions.Timeout:
        st.error("⏱️ Timeout. Arquivo muito grande. Tente novamente em 10 minutos.")
        return None, None, False
    except requests.exceptions.ConnectionError:
        st.error("🔌 Erro de conexão. Verifique internet.")
        return None, None, False
    except Exception as e:
        st.error(f"❌ Erro: {str(e)}")
        return None, None, False


def gerar_arquivo_excel(df, nome_base):
    """Gera arquivo Excel em memória"""
    try:
        output = BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Dados ITP', index=False)
        output.seek(0)
        return output
    except Exception as e:
        st.error(f"❌ Erro ao gerar Excel: {str(e)}")
        return None


# ============================================================================
# INTERFACE PRINCIPAL
# ============================================================================

col1, col2, col3 = st.columns([1, 2, 1])
with col2:
    st.title("🔍 Consulta ITP 2025")

st.markdown("---")
st.write("**Download de questionários por entidade**")
st.markdown("")

# Carregar dados
df_2025, df_2024, sucesso = load_itp_data()

if not sucesso or df_2025 is None or df_2024 is None:
    st.error("❌ Não foi possível carregar os dados.")
    st.stop()

# Preparar dados
todos_estados = set()
if 'estado' in df_2025.columns:
    todos_estados.update(df_2025['estado'].dropna().unique())
if 'estado' in df_2024.columns:
    todos_estados.update(df_2024['estado'].dropna().unique())

todos_estados = sorted(list(todos_estados))

if not todos_estados:
    st.error("❌ Nenhum estado encontrado nos dados")
    st.stop()

# ============================================================================
# ETAPA 1: SELECIONAR ESTADO
# ============================================================================

st.subheader("1️⃣ Selecionar Estado")

sigla_selecionada = st.selectbox(
    "Escolha um estado:",
    options=[""] + todos_estados,
    format_func=lambda x: f"{ESTADOS_MAP.get(x, x)} ({x})" if x else "-- Selecione um estado --",
    key="state_select"
)

if not sigla_selecionada:
    st.info("👆 Selecione um estado para continuar")
    st.stop()

# Filtrar entidades por estado
entidades_2025 = set()
entidades_2024 = set()

if 'estado' in df_2025.columns and 'entidade' in df_2025.columns:
    estado_data = df_2025[df_2025['estado'] == sigla_selecionada]
    entidades_2025 = set(estado_data['entidade'].dropna().unique())

if 'estado' in df_2024.columns and 'entidade' in df_2024.columns:
    estado_data = df_2024[df_2024['estado'] == sigla_selecionada]
    entidades_2024 = set(estado_data['entidade'].dropna().unique())

todas_entidades = sorted(list(entidades_2025.union(entidades_2024)))

if not todas_entidades:
    st.error(f"❌ Nenhuma entidade encontrada para {ESTADOS_MAP.get(sigla_selecionada, sigla_selecionada)}")
    st.stop()

# ============================================================================
# ETAPA 2: BUSCAR ENTIDADE
# ============================================================================

st.subheader("2️⃣ Buscar Entidade")

termo_busca = st.text_input(
    "Digite para filtrar entidades:",
    placeholder="Ex: Prefeitura, Câmara, Assembleia...",
    key="search_input"
)

# Filtrar
entidades_filtradas = [e for e in todas_entidades 
                       if termo_busca.lower() in e.lower()] if termo_busca else todas_entidades

if termo_busca and not entidades_filtradas:
    st.warning(f"⚠️ Nenhuma entidade encontrada para '{termo_busca}'")
    st.stop()

st.caption(f"📊 {len(entidades_filtradas)} entidade(s) disponível(is)")

# ============================================================================
# ETAPA 3: SELECIONAR ENTIDADE
# ============================================================================

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

# Mostrar seleção
st.markdown(f"""
<div class="info-box">
    <strong>✓ Entidade selecionada:</strong><br>
    {entidade_selecionada}
</div>
""", unsafe_allow_html=True)

# ============================================================================
# BOTÕES DE AÇÃO
# ============================================================================

col1, col2 = st.columns(2)

with col1:
    btn_gerar = st.button("📥 Gerar Downloads", use_container_width=True, type="primary")

with col2:
    btn_limpar = st.button("🔄 Limpar", use_container_width=True)

if btn_limpar:
    st.rerun()

# ============================================================================
# GERAR DOWNLOADS
# ============================================================================

if btn_gerar:
    st.markdown("---")
    st.subheader("📊 Arquivos Disponíveis")
    
    with st.spinner('⏳ Processando solicitação...'):
        try:
            # Filtrar dados
            df_2025_filtrado = pd.DataFrame()
            df_2024_filtrado = pd.DataFrame()
            
            if 'estado' in df_2025.columns and 'entidade' in df_2025.columns:
                df_2025_filtrado = df_2025[
                    (df_2025['estado'] == sigla_selecionada) &
                    (df_2025['entidade'] == entidade_selecionada)
                ].copy()
            
            if 'estado' in df_2024.columns and 'entidade' in df_2024.columns:
                df_2024_filtrado = df_2024[
                    (df_2024['estado'] == sigla_selecionada) &
                    (df_2024['entidade'] == entidade_selecionada)
                ].copy()
            
            if df_2025_filtrado.empty and df_2024_filtrado.empty:
                st.error("❌ Nenhum dado encontrado para essa combinação")
                st.stop()
            
            # Gerar arquivos
            arquivos_gerados = {}
            
            if not df_2025_filtrado.empty:
                excel_2025 = gerar_arquivo_excel(df_2025_filtrado, "itp_2025")
                if excel_2025:
                    nome_safe = entidade_selecionada.replace('/', '_')[:40]
                    arquivos_gerados['2025'] = {
                        'data': excel_2025,
                        'nome': f"questionario_itp_2025_{nome_safe}.xlsx",
                        'linhas': len(df_2025_filtrado),
                        'colunas': len(df_2025_filtrado.columns)
                    }
            
            if not df_2024_filtrado.empty:
                excel_2024 = gerar_arquivo_excel(df_2024_filtrado, "itp_2024")
                if excel_2024:
                    nome_safe = entidade_selecionada.replace('/', '_')[:40]
                    arquivos_gerados['2024'] = {
                        'data': excel_2024,
                        'nome': f"questionario_itp_2024_{nome_safe}.xlsx",
                        'linhas': len(df_2024_filtrado),
                        'colunas': len(df_2024_filtrado.columns)
                    }
            
            # Exibir
            if arquivos_gerados:
                st.success(f"✅ Arquivos gerados para: **{entidade_selecionada}**")
                st.markdown("")
                
                for ano in sorted(arquivos_gerados.keys(), reverse=True):
                    info = arquivos_gerados[ano]
                    col1, col2 = st.columns([3, 1])
                    
                    with col1:
                        st.write(f"**📄 ITP {ano}**")
                        st.caption(f"📊 {info['linhas']} linhas • {info['colunas']} colunas")
                    
                    with col2:
                        st.download_button(
                            label="⬇️",
                            data=info['data'],
                            file_name=info['nome'],
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            use_container_width=True
                        )
                    st.markdown("")
                
                st.markdown("---")
                st.markdown("""
                <div class="success-box">
                    <strong>✓ Sucesso!</strong> Clique no botão acima para fazer download.
                </div>
                """, unsafe_allow_html=True)
        
        except Exception as e:
            st.error(f"❌ Erro: {str(e)}")
            import traceback
            st.write(traceback.format_exc())

# ============================================================================
# RODAPÉ
# ============================================================================

st.markdown("---")
col1, col2, col3 = st.columns(3)
with col1:
    st.caption(f"🔄 {datetime.now().strftime('%d/%m/%Y às %H:%M')}")
with col2:
    st.caption("📡 Radar da Transparência")
with col3:
    st.caption("⚡ Otimizado com Parquet")
