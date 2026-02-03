import streamlit as st
import pandas as pd
import zipfile
from io import BytesIO, StringIO
from datetime import datetime
import os
import glob

st.set_page_config(
    page_title="Consulta ITP 2025",
    page_icon="🔍",
    layout="centered"
)

# Arquivos ZIP locais
ZIP_2025_FILES = glob.glob('itp2025_pr*.zip') or glob.glob('*2025*.zip')
ZIP_2024_FILES = glob.glob('itp2024_pr*.zip') or glob.glob('*2024*.zip')

def descompactar_zip(zip_files, ano):
    """Descompacta arquivo ZIP e retorna DataFrame"""
    try:
        if not zip_files:
            return None
        
        zip_file_path = zip_files[0]
        
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            files = zip_ref.namelist()
            
            # Procurar por CSV
            csv_file = None
            for file in files:
                if '.csv' in file.lower():
                    csv_file = file
                    break
            
            if not csv_file:
                return None
            
            # LER CORRETAMENTE DO ZIP
            csv_data = zip_ref.read(csv_file).decode('utf-8')
            df = pd.read_csv(StringIO(csv_data), sep=";", low_memory=False)
            
            return df
    
    except Exception as e:
        st.error(f"❌ Erro ao descompactar {ano}: {str(e)}")
        return None


@st.cache_resource(ttl=86400)
def carregar_dados():
    """Carrega dados dos ZIPs de 2024 e 2025"""
    
    df_2025 = descompactar_zip(ZIP_2025_FILES, 2025)
    df_2024 = descompactar_zip(ZIP_2024_FILES, 2024)
    
    return df_2025, df_2024


def gerar_excel(df, nome_base):
    """Gera Excel em memória"""
    try:
        output = BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Dados', index=False)
        output.seek(0)
        return output
    except Exception as e:
        st.error(f"❌ Erro ao gerar Excel: {e}")
        return None


# ============================================================================
# INTERFACE
# ============================================================================

st.title("🔍 Consulta ITP - Paraná")
st.markdown("---")

with st.spinner("⏳ Carregando dados..."):
    df_2025, df_2024 = carregar_dados()

# Debug
with st.expander("ℹ️ Informações de Debug"):
    st.write(f"**ZIPs 2025:** {ZIP_2025_FILES if ZIP_2025_FILES else '❌ Nenhum'}")
    st.write(f"**ZIPs 2024:** {ZIP_2024_FILES if ZIP_2024_FILES else '❌ Nenhum'}")
    if df_2025 is not None:
        st.write(f"**Linhas 2025:** {len(df_2025)}")
    if df_2024 is not None:
        st.write(f"**Linhas 2024:** {len(df_2024)}")

# Validar dados
if df_2025 is None and df_2024 is None:
    st.error("❌ Não foi possível carregar dados de 2024 nem 2025")
    st.stop()

# Se só tiver um ano, usar esse
if df_2025 is None:
    df = df_2024
    ano_ativo = 2024
elif df_2024 is None:
    df = df_2025
    ano_ativo = 2025
else:
    # Se tiver os dois, usar 2025 como padrão
    df = df_2025
    ano_ativo = 2025

# Garantir apenas PR
df = df[df["uf"] == "PR"].copy()

if df.empty:
    st.error("❌ Não há dados para PR na base carregada.")
    st.stop()

# Lista de entidades
col_entidade = "entidade_nome"
entidades = sorted(df[col_entidade].dropna().unique())

if not entidades:
    st.error("❌ Nenhuma entidade encontrada para PR.")
    st.stop()

st.subheader("1️⃣ Selecionar Ano")

# Botões para escolher ano
col_ano1, col_ano2 = st.columns(2)
with col_ano1:
    if df_2025 is not None:
        if st.button("📅 2025", use_container_width=True):
            st.session_state.ano = 2025
            st.rerun()

with col_ano2:
    if df_2024 is not None:
        if st.button("📅 2024", use_container_width=True):
            st.session_state.ano = 2024
            st.rerun()

# Definir ano ativo
if "ano" not in st.session_state:
    st.session_state.ano = ano_ativo

# Carregar dados do ano escolhido
if st.session_state.ano == 2025:
    if df_2025 is not None:
        df_ano = df_2025[df_2025["uf"] == "PR"].copy()
        ano_texto = "2025"
    else:
        st.error("❌ Dados de 2025 não disponíveis")
        st.stop()
else:
    if df_2024 is not None:
        df_ano = df_2024[df_2024["uf"] == "PR"].copy()
        ano_texto = "2024"
    else:
        st.error("❌ Dados de 2024 não disponíveis")
        st.stop()

st.markdown(f"**Você selecionou: {ano_texto}**")
st.markdown("---")

# Lista de entidades do ano escolhido
entidades_ano = sorted(df_ano[col_entidade].dropna().unique())

st.subheader("2️⃣ Buscar entidade")

termo = st.text_input(
    "Digite parte do nome da entidade:",
    placeholder="Ex: PREFEITURA MUNICIPAL DE CURITIBA",
)

entidades_filtradas = [
    e for e in entidades_ano if termo.lower() in str(e).lower()
] if termo else entidades_ano

if termo and not entidades_filtradas:
    st.warning(f"⚠️ Nenhuma entidade encontrada contendo '{termo}'.")
    st.stop()

st.caption(f"{len(entidades_filtradas)} entidade(s) encontradas")

entidade = st.selectbox(
    "Selecione a entidade:",
    [""] + entidades_filtradas,
    format_func=lambda x: x if x else "-- Selecione --",
)

if not entidade:
    st.info("👆 Digite um termo e selecione uma entidade na lista.")
    st.stop()

col1, col2 = st.columns(2)

with col1:
    gerar = st.button("📥 Gerar planilha", use_container_width=True, type="primary")

with col2:
    limpar = st.button("🔄 Limpar filtros", use_container_width=True)

if limpar:
    st.session_state.ano = ano_ativo
    st.rerun()

if gerar:
    st.markdown("---")
    try:
        df_filtrado = df_ano[df_ano[col_entidade] == entidade].reset_index(drop=True)

        if df_filtrado.empty:
            st.error("❌ Sem dados para essa entidade.")
            st.stop()

        excel = gerar_excel(df_filtrado, f"itp_{ano_texto}_pr")
        if excel:
            st.download_button(
                f"📥 Download ITP {ano_texto} - PR",
                excel,
                f"itp_{ano_texto}_pr_{str(entidade)[:30]}.xlsx",
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
            )

        st.markdown("---")
        st.markdown(f"""
        **✓ Ano**: {ano_texto}  
        **✓ Entidade**: {entidade}  
        **✓ Linhas**: {len(df_filtrado)}  
        **✓ Colunas**: {len(df_filtrado.columns)}
        """)

    except Exception as e:
        st.error(f"❌ Erro ao gerar planilha: {e}")

st.markdown("---")
st.caption(f"🔄 {datetime.now().strftime('%d/%m às %H:%M')} | 📡 ITP 2024 e 2025")
