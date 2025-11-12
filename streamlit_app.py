import streamlit as st

# 1) set_page_config must be first Streamlit command
st.set_page_config(page_title="🔐 Teste de API Key da OpenAI", page_icon="🔐", layout="centered")

# 2) then the rest of the app
st.title("🔐 Teste de API Key da OpenAI")

api_key = st.secrets.get("OPENAI_API_KEY")

if api_key:
    st.success("✅ OPENAI_API_KEY encontrada no st.secrets! Tudo certo 🎉")
else:
    st.error("❌ OPENAI_API_KEY não encontrada. Verifique Settings → Secrets (TOML).")
