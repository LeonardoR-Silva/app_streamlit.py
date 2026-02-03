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

ESTADOS_MAP = {
    'AC': 'Acre', 'AL': 'Alagoas', 'AP': 'Amapá', 'AM': 'Amazonas',
    'BA': 'Bahia', 'CE': 'Ceará', 'DF': 'Distrito Federal', 'ES': 'Espírito Santo',
    'GO': 'Goiás', 'MA': 'Maranhão', 'MT': 'Mato Grosso', 'MS': 'Mato Grosso do Sul',
    'MG': 'Minas Gerais', 'PA': 'Pará', 'PB': 'Paraíba', 'PR': 'Paraná',
    'PE': 'Pernambuco', 'PI': 'Piauí', 'RJ': 'Rio de Janeiro', 'RN': 'Rio Grande do Norte',
    'RS': 'Rio Grande do Sul', 'RO': 'Rondônia', 'RR': 'Roraima', 'SC': 'Santa Catarina',
    'SP': 'São Paulo', 'SE': 'Sergipe', 'TO': 'Tocantins'
}

def descompactar_zip(zip_files, ano):
    """Descompacta arquivo ZIP e retorna DataFrame - CORRIGIDO"""
    try:
        if not zip_files:
            st.error(f"❌ Arquivo ZIP para {ano} não encontrado")
            return None
        
        zip_file_path = zip_files[0]
        st.info(f"⏳ Descompactando {ano}...")
        
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            files = zip_ref.namelist()
            st.write(f"📦 Encontrados {len(files)} arquivo(s)")
            
            # Procurar por CSV
            csv_file = None
            for file in files:
                if '.csv' in file.lower():
                    csv_file = file
                    break
            
            if not csv_file:
                st.error(f"❌ Nenhum CSV encontrado em {zip_file_path}")
                st.write(f"Arquivos: {files}")
                return None
            
            st.write(f"📂 Lendo: {csv_file}")
            
            # LER CORRETAMENTE DO ZIP
            csv_data = zip_ref.read(csv_file).decode('utf-8')
            df = pd.read_csv(StringIO(csv_data), sep=";", low_memory=False)
            
            st.success(f"✅ {ano} carregado! ({len(df)} linhas)")
            return df
    
    except Exception as e:
        st.error(f"❌ Erro ao descompactar {ano}: {str(e)}")
        import traceback
        st.write(traceback.format_exc())
        return None


@st.cache_resource(ttl=86400)
def carregar_dados():
    """Carrega dados dos ZIPs locais"""
    
    st.info("⚡ Carregando dados do repositório...")
    
    df_2025 = descompactar_zip(ZIP_2025_FILES, 2025)
    df_2024 = descompactar_zip(ZIP_2024_FILES, 2024)
    
    if df_2025 is None and df_2024 is None:
        st.error("❌ Não foi possível carregar dados")
        return None, None, False
    
    if df_2025 is None:
        df_2025 = df_2024.copy() if df_2024 is not None else None
    if df_2024 is None:
        df_2024 = df_2025.copy() if df_2025 is not None else None
    
    return df_2025, df_2024, True


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

st.title("🔍 Consulta ITP 2025 - Paraná")
st.markdown("---")

with st.expander("ℹ️ Informações de Debug"):
    st.write(f"**ZIPs 2025:** {ZIP_2025_FILES if ZIP_2025_FILES else '❌ Nenhum'}")
    st.write(f"**ZIPs 2024:** {ZIP_2024_FILES if ZIP_2024_FILES else '❌ Nenhum'}")
    st.write(f"**Colunas do dataframe:** {list(df.columns)}")

# Garante apenas PR
df = df[df["uf"] == "PR"].copy()

if df.empty:
    st.error("❌ Não há dados para PR na base carregada.")
    st.stop()

# Vamos usar 'entidade_nome' como chave de busca
col_entidade = "entidade_nome"

entidades = sorted(df[col_entidade].dropna().unique())

if not entidades:
    st.error("❌ Nenhuma entidade encontrada para PR.")
    st.stop()

st.subheader("1️⃣ Buscar entidade")

termo = st.text_input(
    "Digite parte do nome da entidade (ex.: Prefeitura, Câmara, etc.):",
    placeholder="Ex: PREFEITURA MUNICIPAL DE CURITIBA",
)

entidades_filtradas = [
    e for e in entidades if termo.lower() in str(e).lower()
] if termo else entidades

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
    st.rerun()

if gerar:
    st.markdown("---")
    try:
        df_filtrado = df[df[col_entidade] == entidade].reset_index(drop=True)

        if df_filtrado.empty:
            st.error("❌ Sem dados para essa entidade.")
            st.stop()

        excel = gerar_excel(df_filtrado, "itp_2025_pr")
        if excel:
            st.download_button(
                "📥 Download ITP 2025 - PR",
                excel,
                f"itp_2025_pr_{str(entidade)[:30]}.xlsx",
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
            )

        st.markdown("---")
        st.markdown(f"""
        **✓ Entidade**: {entidade}  
        **✓ Linhas**: {len(df_filtrado)}  
        **✓ Colunas**: {len(df_filtrado.columns)}
        """)

    except Exception as e:
        st.error(f"❌ Erro ao gerar planilha: {e}")

st.markdown("---")
st.caption(f"🔄 {datetime.now().strftime('%d/%m às %H:%M')} | 📡 Dados do Paraná (PR)")
