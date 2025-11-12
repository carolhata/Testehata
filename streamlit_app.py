import os
import re
import json
import logging
from io import BytesIO
from urllib.parse import urlparse
from typing import List, Literal, Optional, Dict, Any
from datetime import datetime, timezone

import streamlit as st
import pandas as pd
from pydantic import BaseModel, Field, PositiveInt, validator

# =========================
# Configuração básica
# =========================
st.set_page_config(
    page_title="Meu ChatGPT + Web (Tavily) + Raspagem HTML",
    page_icon="🧠",
    layout="centered"
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("chatgpt-web-scraper-app")

# =========================
# Secrets seguros
# =========================
def get_secret(name: str) -> Optional[str]:
    try:
        return st.secrets.get(name, None)
    except Exception:
        return None

OPENAI_API_KEY = get_secret("OPENAI_API_KEY")
TAVILY_API_KEY = get_secret("TAVILY_API_KEY")

if not OPENAI_API_KEY:
    st.error("Defina OPENAI_API_KEY em Secrets antes de usar.")
    st.stop()

os.environ["OPENAI_API_KEY"] = OPENAI_API_KEY
from openai import OpenAI
client = OpenAI()

# =========================
# Modelos e validação
# =========================
AVAILABLE_MODELS = [
    "gpt-4o-mini",
    "gpt-4o",
    "gpt-4.1-mini",
    "gpt-4.1"
]

class AppSettings(BaseModel):
    model: Literal["gpt-4o-mini", "gpt-4o", "gpt-4.1-mini", "gpt-4.1"] = Field(default="gpt-4o-mini")
    temperature: float = Field(default=0.3, ge=0.0, le=2.0)
    max_tokens: PositiveInt = Field(default=1024)
    system_prompt: str = Field(
        default=(
            "Você é um assistente de IA útil, objetivo e seguro. "
            "Responda em português do Brasil, de forma clara e prática."
        )
    )
    use_tavily: bool = Field(default=True)
    tavily_depth: Literal["basic", "advanced"] = Field(default="basic")
    tavily_max_results: PositiveInt = Field(default=5)
    inject_scrape: bool = Field(default=True)
    scrape_char_limit: PositiveInt = Field(default=8000)

    @validator("max_tokens")
    def clamp_max_tokens(cls, v):
        return min(v, 4096)

# =========================
# Estado da sessão
# =========================
if "history" not in st.session_state:
    st.session_state.history = []
if "settings" not in st.session_state:
    st.session_state.settings = AppSettings().dict()
if "scrape_context" not in st.session_state:
    st.session_state.scrape_context = ""
if "scrape_url" not in st.session_state:
    st.session_state.scrape_url = ""
if "scrape_title" not in st.session_state:
    st.session_state.scrape_title = ""

# =========================
# Utilidades comuns
# =========================
def normalize_whitespace(text: str) -> str:
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n\s*\n+", "\n\n", text)
    return text.strip()

def build_scrape_dataframe() -> Optional[pd.DataFrame]:
    txt = (st.session_state.scrape_context or "").strip()
    url = st.session_state.scrape_url or ""
    title = st.session_state.scrape_title or ""
    if not txt:
        return None
    # separa por parágrafos em blocos não vazios
    parts = [p.strip() for p in re.split(r"\n{2,}", txt) if p.strip()]
    rows = []
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    for i, p in enumerate(parts, start=1):
        rows.append(
            {
                "timestamp_utc": ts,
                "url": url,
                "title": title,
                "paragraph_index": i,
                "paragraph_length": len(p),
                "text": p,
            }
        )
    return pd.DataFrame(rows)

def df_to_excel_bytes(df: pd.DataFrame, sheet_name: str = "scrape") -> bytes:
    bio = BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
    bio.seek(0)
    return bio.read()

def render_excel_download_button_in_sidebar():
    """Cria (ou atualiza) o botão de download no sidebar conforme o estado atual."""
    with st.sidebar:
        st.markdown("### Exportação")
        df_preview = build_scrape_dataframe()
        if df_preview is None or df_preview.empty:
            st.caption("Nenhum conteúdo raspado disponível para exportação.")
            return
        parsed = urlparse(st.session_state.scrape_url or "")
        host = (parsed.netloc or "conteudo").replace(":", "_")
        ts_fname = datetime.now().strftime("%Y%m%d-%H%M%S")
        fname = f"raspagem_{host}_{ts_fname}.xlsx"
        st.download_button(
            label="Baixar Excel da raspagem",
            data=df_to_excel_bytes(df_preview),
            file_name=fname,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True
        )

# =========================
# Sidebar de Configuração
# =========================
with st.sidebar:
    st.markdown("## Configurações")
    model = st.selectbox("Modelo", AVAILABLE_MODELS, index=0, key="model_select")
    temperature = st.slider("Temperatura", 0.0, 2.0, float(st.session_state.settings["temperature"]), 0.1)
    max_tokens = st.number_input(
        "Máx tokens de saída", min_value=64, max_value=4096,
        value=int(st.session_state.settings["max_tokens"]), step=64
    )
    system_prompt = st.text_area(
        "System Prompt",
        value=st.session_state.settings["system_prompt"],
        height=140
    )

    st.markdown("### Busca online Tavily")
    use_tavily = st.checkbox(
        "Ativar busca online",
        value=bool(st.session_state.settings.get("use_tavily", True))
    )
    tavily_depth = st.selectbox(
        "Profundidade", ["basic", "advanced"],
        index=0 if st.session_state.settings.get("tavily_depth") == "basic" else 1
    )
    tavily_max_results = st.slider(
        "Máx resultados", 1, 10,
        int(st.session_state.settings.get("tavily_max_results", 5))
    )

    st.markdown("### Raspagem de HTML")
    inject_scrape = st.checkbox(
        "Injetar conteúdo raspado na conversa",
        value=bool(st.session_state.settings.get("inject_scrape", True))
    )
    scrape_char_limit = st.slider(
        "Limite de caracteres do texto raspado",
        min_value=1000, max_value=20000,
        value=int(st.session_state.settings.get("scrape_char_limit", 8000)),
        step=500
    )

    # Botão de limpar histórico
    st.markdown("---")
    if st.button("Limpar histórico"):
        st.session_state.history = []
        st.success("Histórico limpo.")

# cria/atualiza o botão de download com o estado atual
render_excel_download_button_in_sidebar()

# Atualiza objeto de configuração validado (fora do with para evitar conflitos de estado)
try:
    cfg = AppSettings(
        model=model,
        temperature=temperature,
        max_tokens=int(max_tokens),
        system_prompt=system_prompt,
        use_tavily=use_tavily,
        tavily_depth=tavily_depth,  # type: ignore
        tavily_max_results=int(tavily_max_results),
        inject_scrape=inject_scrape,
        scrape_char_limit=int(scrape_char_limit)
    )
    st.session_state.settings = cfg.dict()
except Exception as e:
    st.error(f"Configuração inválida: {e}")

# =========================
# Cabeçalho
# =========================
st.title("🧠 Meu ChatGPT com Tavily + Raspagem HTML")
st.caption("Raspagem direta com fallback para Tavily Extract e Jina Reader. Exportação para Excel no sidebar.")

# =========================
# Utilidades de IA e Web
# =========================
def openai_stream_chat(messages: List[dict], model: str, temperature: float, max_tokens: int, system_prompt: str):
    full_messages = [{"role": "system", "content": system_prompt}] + messages
    stream = client.chat.completions.create(
        model=model,
        messages=full_messages,
        temperature=temperature,
        max_tokens=max_tokens,
        stream=True
    )
    partial = ""
    placeholder = st.empty()
    for chunk in stream:
        delta = chunk.choices[0].delta
        if delta and getattr(delta, "content", None):
            partial += delta.content
            placeholder.markdown(partial)
    return partial

def tavily_search(query: str, depth: str = "basic", max_results: int = 5) -> Dict[str, Any]:
    if not TAVILY_API_KEY:
        return {"error": "TAVILY_API_KEY não definida em Secrets."}
    import requests
    url = "https://api.tavily.com/search"
    payload = {
        "api_key": TAVILY_API_KEY,
        "query": query,
        "search_depth": depth,
        "include_images": False,
        "include_answer": True,
        "max_results": int(max_results)
    }
    try:
        resp = requests.post(url, json=payload, timeout=45)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logger.exception("Erro Tavily (search)")
        return {"error": f"Erro ao consultar Tavily: {e}"}

def tavily_extract(url_to_fetch: str) -> Dict[str, Any]:
    if not TAVILY_API_KEY:
        return {"ok": False, "title": "", "content": "", "error": "TAVILY_API_KEY não definida.", "raw": ""}
    import requests
    api = "https://api.tavily.com/extract"
    payload = {"api_key": TAVILY_API_KEY, "url": url_to_fetch}
    try:
        resp = requests.post(api, json=payload, timeout=45)
        raw = resp.text
        if resp.status_code >= 400:
            return {"ok": False, "title": "", "content": "", "error": f"HTTP {resp.status_code}: {raw[:400]}", "raw": raw}
        data = resp.json() if raw else {}
        title = data.get("title") or ""
        content = data.get("content") or data.get("text") or ""
        if not content:
            return {"ok": False, "title": title, "content": "", "error": "Sem conteúdo retornado pela Tavily.", "raw": raw}
        return {"ok": True, "title": title, "content": content, "error": None, "raw": raw}
    except Exception as e:
        logger.exception("Erro Tavily (extract)")
        return {"ok": False, "title": "", "content": "", "error": f"Falha no Tavily Extract: {e}", "raw": ""}

def jina_reader_fetch(url_to_fetch: str) -> Dict[str, Any]:
    import requests
    parsed = urlparse(url_to_fetch)
    if not parsed.scheme:
        url_to_fetch = "https://" + url_to_fetch
        parsed = urlparse(url_to_fetch)
    reader_url = f"https://r.jina.ai/{parsed.scheme}://{parsed.netloc}{parsed.path or ''}"
    if parsed.query:
        reader_url += f"?{parsed.query}"
    try:
        resp = requests.get(reader_url, timeout=45)
        text = resp.text or ""
        if resp.status_code >= 400 or not text.strip():
            return {"ok": False, "title": "", "content": "", "error": f"Reader HTTP {resp.status_code}"}
        return {"ok": True, "title": "", "content": text, "error": None}
    except Exception as e:
        logger.exception("Erro Jina Reader")
        return {"ok": False, "title": "", "content": "", "error": f"Falha no Jina Reader: {e}"}

def scrape_direct(url: str, user_agent: str = "Mozilla/5.0 (compatible; StreamlitScraper/1.0)") -> Dict[str, Any]:
    import requests
    from bs4 import BeautifulSoup
    parsed = urlparse(url)
    if not parsed.scheme:
        url = "https://" + url
    headers = {
        "User-Agent": user_agent,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,/;q=0.8",
        "Accept-Language": "en-US,en;q=0.9,pt-BR;q=0.8",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Connection": "close",
        "Upgrade-Insecure-Requests": "1",
    }
    try:
        resp = requests.get(url, headers=headers, timeout=45)
        content_type = resp.headers.get("Content-Type", "")
        status = resp.status_code
        if status >= 400:
            return {"ok": False, "title": "", "text": "", "error": f"HTTP {status}", "status_code": status}
        if "text/html" not in content_type and "application/xhtml+xml" not in content_type:
            return {"ok": False, "title": "", "text": "", "error": f"Conteúdo não-HTML: {content_type}", "status_code": status}
        try:
            soup = BeautifulSoup(resp.text, "lxml")
        except Exception:
            soup = BeautifulSoup(resp.text, "html.parser")
        for tag in soup(["script", "style", "noscript", "template"]):
            tag.decompose()
        title = (soup.title.string if soup.title else "") or ""
        text = normalize_whitespace(soup.get_text(separator="\n"))
        return {"ok": True, "title": title.strip(), "text": text, "error": None, "status_code": status}
    except Exception as e:
        logger.exception("Erro no download direto")
        return {"ok": False, "title": "", "text": "", "error": f"Falha ao baixar HTML: {e}", "status_code": None}

def scrape_url_to_text(url: str, char_limit: int = 8000) -> Dict[str, Any]:
    """
    Estratégia em camadas:
      1) Direto
      2) Tavily Extract (se chave)
      3) Jina Reader
    """
    if not url or not isinstance(url, str):
        return {"ok": False, "url": url, "title": "", "text": "", "error": "URL inválida."}

    direct = scrape_direct(url)
    if direct.get("ok"):
        text = direct["text"]
        if len(text) > char_limit:
            text = text[:char_limit] + "\n\n[...conteúdo truncado…]"
        st.session_state.scrape_title = direct["title"]
        return {"ok": True, "url": url, "title": direct["title"], "text": text, "error": None}

    if TAVILY_API_KEY:
        te = tavily_extract(url)
        if te.get("ok"):
            text = normalize_whitespace(te["content"])
            if len(text) > char_limit:
                text = text[:char_limit] + "\n\n[...conteúdo truncado…]"
            st.session_state.scrape_title = te.get("title") or ""
            return {"ok": True, "url": url, "title": st.session_state.scrape_title or url, "text": text, "error": None}
        else:
            st.session_state["_last_tavily_extract_error"] = te.get("error")

    jr = jina_reader_fetch(url)
    if jr.get("ok"):
        text = normalize_whitespace(jr["content"])
        if len(text) > char_limit:
            text = text[:char_limit] + "\n\n[...conteúdo truncado…]"
        st.session_state.scrape_title = jr.get("title") or ""
        return {"ok": True, "url": url, "title": st.session_state.scrape_title or url, "text": text, "error": None}

    return {"ok": False, "url": url, "title": "", "text": "", "error": jr.get("error") or direct.get("error") or "Falha desconhecida"}

def render_history(messages: List[dict]):
    for m in messages:
        if m["role"] == "user":
            with st.chat_message("user"):
                st.markdown(m["content"])
        else:
            with st.chat_message("assistant"):
                st.markdown(m["content"])

def inject_web_context_if_enabled(user_msg: str) -> Optional[str]:
    s = st.session_state.settings
    if not s.get("use_tavily", True):
        return None
    data = tavily_search(
        query=user_msg,
        depth=s.get("tavily_depth", "basic"),
        max_results=int(s.get("tavily_max_results", 5))
    )
    if "error" in data:
        return f"(Falha ao obter contexto da web: {data['error']})"
    return format_tavily_context(data)

def format_tavily_context(data: Dict[str, Any]) -> str:
    answer = data.get("answer") or ""
    results = data.get("results", []) or []
    top = results[:5]
    lines = []
    for i, r in enumerate(top, start=1):
        title = r.get("title") or "Fonte"
        url = r.get("url") or ""
        snippet = (r.get("content") or "").strip()
        if len(snippet) > 220:
            snippet = snippet[:217] + "..."
        lines.append(f"{i}. *{title}* — {snippet}\n   {url}")
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    header = f"### Contexto de pesquisa web (Tavily) — {ts}\n"
    if answer:
        header += f"\n*Síntese:* {answer}\n"
    if lines:
        header += "\n*Fontes:*\n" + "\n".join(lines)
    else:
        header += "\n(Nenhuma fonte relevante retornada.)"
    return header

# =========================
# Bloco: Raspagem de HTML
# =========================
st.subheader("Raspagem de HTML (cole uma URL)")
col1, col2 = st.columns([4, 1])
with col1:
    url_input = st.text_input(
        "URL a raspar",
        value=st.session_state.scrape_url,
        placeholder="https://exemplo.com/pagina"
    )
with col2:
    scrape_btn = st.button("Raspar", use_container_width=True)

if scrape_btn and url_input:
    st.session_state.scrape_url = url_input
    with st.spinner("Extraindo conteúdo da página..."):
        res = scrape_url_to_text(url_input, char_limit=st.session_state.settings["scrape_char_limit"])
    if not res.get("ok"):
        err = res.get("error") or "Falha desconhecida"
        extra = st.session_state.pop("_last_tavily_extract_error", None)
        if extra:
            err += f"\n\nDetalhe Tavily Extract: {extra}"
        st.error(f"Falha na raspagem: {err}")
    else:
        st.session_state.scrape_context = res["text"]
        st.success(f"Conteúdo raspado de: {res.get('title') or res.get('url')}")
        with st.expander("Prévia do texto raspado"):
            st.write(res["text"])
        # 👉 Garante que o sidebar se atualize já com o conteúdo raspado
        st.rerun()

# =========================
# Área de chat
# =========================
st.markdown("---")
render_history(st.session_state.history)

user_input = st.chat_input("Digite sua mensagem...")
if user_input:
    st.session_state.history.append({"role": "user", "content": user_input})
    with st.chat_message("user"):
        st.markdown(user_input)

    s = st.session_state.settings
    if s.get("inject_scrape", True) and st.session_state.scrape_context:
        scraped_md = "### Contexto de página (raspado via URL)\n\n" + st.session_state.scrape_context
        with st.chat_message("assistant"):
            st.markdown(scraped_md)
        st.session_state.history.append({"role": "assistant", "content": scraped_md})

    web_ctx_md = inject_web_context_if_enabled(user_input)
    if web_ctx_md:
        with st.chat_message("assistant"):
            st.markdown(web_ctx_md)
        st.session_state.history.append({"role": "assistant", "content": f"(Contexto externo adicionado da web)\n\n{web_ctx_md}"})

    with st.chat_message("assistant"):
        try:
            reply = openai_stream_chat(
                messages=st.session_state.history,
                model=s["model"],
                temperature=s["temperature"],
                max_tokens=s["max_tokens"],
                system_prompt=s["system_prompt"]
            )
            st.session_state.history.append({"role": "assistant", "content": reply})
        except Exception as e:
            logger.exception("Erro ao gerar resposta")
            st.error(f"Ocorreu um erro ao chamar o modelo: {e}")

# =========================
# Diagnóstico
# =========================
with st.expander("Diagnóstico técnico"):
    s = st.session_state.settings
    st.json(
        {
            "model": s["model"],
            "temperature": s["temperature"],
            "max_tokens": s["max_tokens"],
            "history_len": len(st.session_state.history),
            "use_tavily": s["use_tavily"],
            "tavily_depth": s["tavily_depth"],
            "tavily_max_results": s["tavily_max_results"],
            "inject_scrape": s["inject_scrape"],
            "scrape_char_limit": s["scrape_char_limit"],
            "tem_scrape_context": bool(st.session_state.scrape_context),
            "streamlit_cloud_ready": True
        }
    )

st.caption(
    "Secrets necessários: OPENAI_API_KEY e (opcional) TAVILY_API_KEY. "
    "Nenhuma chave é armazenada no código."
)
