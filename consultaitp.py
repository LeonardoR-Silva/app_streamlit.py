import streamlit as st
import pandas as pd
import zipfile
import requests
from io import BytesIO, StringIO
from datetime import datetime
import warnings
import os
import time

warnings.filterwarnings('ignore')

# ============================================================================
# CONFIGURAÇÃO
# ============================================================================

st.set_page_config(
    page_title="Consulta ITP 2025",
    page_icon="🔍",
    layout="centered"
)

URL_2025 = 'https://radardatransparencia.atricon.org.br/dados/dados_pntp_2025.zip'
URL_2024 = 'https://radardatransparencia.atricon.org.br/dados/dados_pntp_2024.zip'

CACHE_DIR = '.cache_itp'
CACHE_2025 = os.path.join(CACHE_DIR, 'itp_2025.parquet')
CACHE_2024 = os.path.join(CACHE_DIR, 'itp_2024.parquet')

os.makedirs(CACHE_DIR, exist_ok=True)

ESTADOS_MAP = {
    'AC': 'Acre', 'AL': 'Alagoas', 'AP': 'Amapá', 'AM': 'Amazonas',
    'BA': 'Bahia', 'CE': 'Ceará', 'DF': 'Distrito Federal', 'ES': 'Espírito Santo',
    'GO': 'Goiás', 'MA': 'Maranhão', 'MT': 'Mato Grosso', 'MS': 'Mato Grosso do Sul',
    'MG': 'Minas Gerais', 'PA': 'Pará', 'PB': 'Paraíba', 'PR': 'Paraná',
    'PE': 'Pernambuco', 'PI': 'Piauí', 'RJ': 'Rio de Janeiro', 'RN': 'Rio Grande do Norte',
    'RS': 'Rio Grande do Sul', 'RO': 'Rondônia', 'RR': 'Roraima', 'SC': 'Santa Catarina',
    'SP': 'São Paulo', 'SE': 'Sergipe', 'TO': 'Tocantins'
}

# ============================================================================
# FUNÇÕES DE CARREGAMENTO
# ============================================================================

def baixar_e_processar(url, cache_path, ano):
    """Baixa, processa e salva em cache"""
    try:
        st.write(f"📥 Baixando ITP {ano}...")
        
        # Usar session para reutilizar conexão
        session = requests.Session()
        session.headers.update({'User-Agent': 'Mozilla/5.0'})
        
        response = session.get(url, timeout=900, stream=True)
        response.raise_for_status()
        
        st.write(f"📂 Extraindo ITP {ano}...")
        
        zip_file = zipfile.ZipFile(BytesIO(response.content))
        csv_content = None
        
        for file_info in zip_file.filelist:
            if f'respostas_avaliacoes_pntp_{ano}.csv' in file_info.filename:
                csv_content = zip_file.read(file_info).decode('utf-8')
                break
        
        if not csv_content:
            st.error(f"❌ CSV {ano} não encontrado no ZIP")
            return None
        
        st.write(f"📊 Processando ITP {ano}...")
        
        df = pd.read_csv(
            StringIO(csv_content),
            sep=";",
            dtype={'estado': 'category', 'entidade': 'string'},
            low_memory=False
        )
        
        st.write(f"💾 Salvando cache ITP {ano}...")
        df.to_parquet(cache_path, compression='snappy', index=False)
        
        size_mb = os.path.getsize(cache_path) / (1024*1024)
        st.write(f"✅ ITP {ano} salvo ({size_mb:.1f}MB)")
        
        return df
    
    except requests.exceptions.Timeout:
        st.error(f"⏱️ Timeout ao baixar ITP {ano}")
        return None
    except requests.exceptions.ConnectionError:
        st.error(f"🔌 Erro de conexão ao baixar ITP {ano}")
        return None
    except Exception as e:
        st.error(f"❌ Erro ao processar ITP {ano}: {str(e)}")
        return None


@st.cache_resource(ttl=86400)
def carregar_dados():
    """Carrega dados com fallback"""
    df_2025 = None
    df_2024 = None
    
    # Tentar carregar cache
    if os.path.exists(CACHE_2025) and os.path.exists(CACHE_2024):
        try:
            st.info("⚡ Carregando cache local...")
            df_2025 = pd.read_parquet(CACHE_2025)
            df_2024 = pd.read_parquet(CACHE_2024)
            st.success("✅ Cache carregado!")
            return df_2025, df_2024, True
        except Exception as e:
            st.warning(f"⚠️ Erro ao carregar cache: {e}")
    
    # Cache não existe, tentar baixar
    if df_2025 is None:
        df_2025 = baixar_e_processar(URL_2025, CACHE_2025, 2025)
    
    if df_2024 is None:
        df_2024 = baixar_e_processar(URL_2024, CACHE_2024, 2024)
    
    if df_2025 is None or df_2024 is None:
        st.error("❌ Não foi possível carregar os dados.")
        return None, None, False
    
    return df_2025, df_2024, True


def gerar_excel(df, nome_base):
    """Gera Excel em memória"""
    try:
        from io import BytesIO
        output = BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Dados', index=False)
        output.seek(0)
        return output
    except Exception as e:
        st.error(f"❌ Erro: {e}")
        return None


# ============================================================================
# INTERFACE
# ============================================================================

st.title("🔍 Consulta ITP 2025")
st.markdown("---")

# Carregar dados
df_2025, df_2024, sucesso = carregar_dados()

if not sucesso:
    st.stop()

# Preparar estados
todos_estados = set()
if 'estado' in df_2025.columns:
    todos_estados.update(df_2025['estado'].dropna().unique())
if 'estado' in df_2024.columns:
    todos_estados.update(df_2024['estado'].dropna().unique())

todos_estados = sorted(list(todos_estados))

# ============================================================================
# SELEÇÃO DE ESTADO
# ============================================================================

st.subheader("1️⃣ Estado")
estado = st.selectbox(
    "Selecione:",
    [""] + todos_estados,
    format_func=lambda x: f"{ESTADOS_MAP.get(x, x)} ({x})" if x else "-- Selecione --",
    key="state"
)

if not estado:
    st.info("👆 Selecione um estado")
    st.stop()

# Filtrar entidades
entidades = set()
if 'estado' in df_2025.columns and 'entidade' in df_2025.columns:
    entidades.update(df_2025[df_2025['estado'] == estado]['entidade'].dropna().unique())
if 'estado' in df_2024.columns and 'entidade' in df_2024.columns:
    entidades.update(df_2024[df_2024['estado'] == estado]['entidade'].dropna().unique())

entidades = sorted(list(entidades))

if not entidades:
    st.error(f"❌ Sem entidades para {estado}")
    st.stop()

# ============================================================================
# BUSCA E SELEÇÃO DE ENTIDADE
# ============================================================================

st.subheader("2️⃣ Entidade")

termo = st.text_input(
    "Buscar:",
    placeholder="Ex: Prefeitura...",
    key="search"
)

entidades_filtradas = [e for e in entidades if termo.lower() in e.lower()] if termo else entidades

if termo and not entidades_filtradas:
    st.warning(f"⚠️ Sem resultados para '{termo}'")
    st.stop()

st.caption(f"{len(entidades_filtradas)} entidade(s)")

entidade = st.selectbox(
    "Selecione:",
    [""] + entidades_filtradas,
    format_func=lambda x: x if x else "-- Selecione --",
    key="entity"
)

if not entidade:
    st.info("👆 Selecione uma entidade")
    st.stop()

# ============================================================================
# BOTÕES
# ============================================================================

col1, col2 = st.columns(2)

with col1:
    gerar = st.button("📥 Gerar", use_container_width=True, type="primary")

with col2:
    limpar = st.button("🔄 Limpar", use_container_width=True)

if limpar:
    st.rerun()

# ============================================================================
# GERAR DOWNLOADS
# ============================================================================

if gerar:
    st.markdown("---")
    
    try:
        # Filtrar dados
        df_2025_filtrado = pd.DataFrame()
        df_2024_filtrado = pd.DataFrame()
        
        if 'estado' in df_2025.columns and 'entidade' in df_2025.columns:
            df_2025_filtrado = df_2025[
                (df_2025['estado'] == estado) &
                (df_2025['entidade'] == entidade)
            ].reset_index(drop=True)
        
        if 'estado' in df_2024.columns and 'entidade' in df_2024.columns:
            df_2024_filtrado = df_2024[
                (df_2024['estado'] == estado) &
                (df_2024['entidade'] == entidade)
            ].reset_index(drop=True)
        
        if df_2025_filtrado.empty and df_2024_filtrado.empty:
            st.error("❌ Sem dados para essa combinação")
            st.stop()
        
        # Gerar arquivos
        if not df_2025_filtrado.empty:
            excel = gerar_excel(df_2025_filtrado, "itp_2025")
            if excel:
                st.download_button(
                    "📥 ITP 2025",
                    excel,
                    f"itp_2025_{entidade[:30]}.xlsx",
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True
                )
        
        if not df_2024_filtrado.empty:
            excel = gerar_excel(df_2024_filtrado, "itp_2024")
            if excel:
                st.download_button(
                    "📥 ITP 2024",
                    excel,
                    f"itp_2024_{entidade[:30]}.xlsx",
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True
                )
        
        st.success("✅ Arquivos prontos!")
    
    except Exception as e:
        st.error(f"❌ Erro: {e}")

# ============================================================================
# RODAPÉ
# ============================================================================

st.markdown("---")
st.caption(f"🔄 {datetime.now().strftime('%d/%m às %H:%M')} | 📡 Radar")
