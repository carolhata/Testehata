# streamlit_app.py
import os
import re
import json
import logging
from io import BytesIO
from urllib.parse import urlparse
from typing import List, Optional, Dict, Any
from datetime import datetime, timezone

import streamlit as st
import pandas as pd
import requests
from bs4 import BeautifulSoup
from pydantic import BaseModel, Field, PositiveInt, validator

# -------------------------
# Config
# -------------------------
st.set_page_config(page_title="Zap Imóveis — API POST Extractor", page_icon="🏠", layout="wide")
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("zap-post-scraper")

# -------------------------
# Settings / Defaults
# -------------------------
class AppSettings(BaseModel):
    max_results: PositiveInt = Field(default=100)
    page_size: PositiveInt = Field(default=50)
    timeout_sec: PositiveInt = Field(default=20)

    @validator("max_results")
    def clamp_max(cls, v):
        return min(v, 1000)

# Ensure settings exist in session_state and are valid
if "settings" not in st.session_state:
    st.session_state["settings"] = AppSettings().dict()
else:
    # validate/repair if needed
    try:
        _ = AppSettings(**st.session_state.get("settings", {}))
    except Exception:
        st.session_state["settings"] = AppSettings().dict()

# Helper to get a setting with fallback
def get_setting(name: str, fallback: Any):
    return st.session_state.get("settings", {}).get(name, fallback)

HEADERS_BASE = {
    "User-Agent": "Mozilla/5.0 (compatible; StreamlitScraper/1.0)",
    "Accept": "application/json, text/plain, */*",
    "Content-Type": "application/json",
    # Zap usually expects this header:
    "X-Application-Name": "zap-web-desktop",
    "Referer": "https://www.zapimoveis.com.br/"
}

# -------------------------
# Helpers
# -------------------------
def safe_post(url: str, json_payload: dict, timeout: int = 20, extra_headers: Optional[dict] = None) -> Optional[requests.Response]:
    try:
        headers = HEADERS_BASE.copy()
        if extra_headers:
            headers.update(extra_headers)
        resp = requests.post(url, json=json_payload, headers=headers, timeout=timeout)
        resp.raise_for_status()
        return resp
    except Exception as e:
        logger.debug(f"safe_post failed {url}: {e}", exc_info=True)
        return None

def safe_get(url: str, timeout: int = 20, extra_headers: Optional[dict] = None) -> Optional[requests.Response]:
    try:
        headers = HEADERS_BASE.copy()
        if extra_headers:
            headers.update(extra_headers)
        headers.pop("Content-Type", None)
        resp = requests.get(url, headers=headers, timeout=timeout)
        resp.raise_for_status()
        return resp
    except Exception as e:
        logger.debug(f"safe_get failed {url}: {e}", exc_info=True)
        return None

def build_payload_for_zap(city: Optional[str], neighborhood: Optional[str], area_min: Optional[int], area_max: Optional[int], page: int = 1, size: int = 50) -> dict:
    payload = {
        "business": ["SALE"],
        "category": ["APARTMENT"],
        "page": max(0, page - 1),
        "size": max(1, size),
        "filters": []
    }
    if city:
        payload["filters"].append({"name": "addressCity", "value": [city]})
    if neighborhood:
        payload["filters"].append({"name": "addressNeighborhood", "value": [neighborhood]})
    if area_min or area_max:
        rf = {"name": "usableArea", "range": {}}
        if area_min:
            rf["range"]["min"] = int(area_min)
        if area_max:
            rf["range"]["max"] = int(area_max)
        payload["filters"].append(rf)
    return payload

def extract_listing_from_zap_item(item: Dict[str,Any]) -> Dict[str,Any]:
    out = {"Endereço": None, "Valor": None, "Condominio": None, "IPTU": None,
           "M2": None, "Quartos": None, "Suites": None, "Vagas": None, "Link": None}
    if not isinstance(item, dict):
        return out

    def g(*keys):
        for k in keys:
            if k in item and item[k] is not None:
                return item[k]
        return None

    link = g("absoluteUrl", "url", "link", "listingUrl")
    if link:
        out["Link"] = link if link.startswith("http") else ("https://www.zapimoveis.com.br" + link)

    price_obj = g("price", "businessPrice", "priceSpecification", "pricing", "value")
    if isinstance(price_obj, dict):
        p = price_obj.get("value") or price_obj.get("amount") or price_obj.get("price")
        if p is not None:
            out["Valor"] = f"R$ {p}"
    elif price_obj is not None:
        out["Valor"] = str(price_obj)

    condo = g("condominiumFee", "condominium", "condominiumFeeFormatted")
    if condo:
        out["Condominio"] = str(condo)
    iptu = g("iptuFee", "iptu", "iptuFormatted")
    if iptu:
        out["IPTU"] = str(iptu)

    area = g("usableArea", "floorArea", "area", "size")
    if isinstance(area, dict):
        av = area.get("value") or area.get("amount") or area.get("size")
        if av:
            out["M2"] = str(av)
    elif area is not None:
        out["M2"] = str(area)

    beds = g("bedrooms", "bedroom", "numberBedrooms")
    if beds is not None:
        out["Quartos"] = str(beds)
    suites = g("suites", "suitesNumber", "numberOfSuites")
    if suites is not None:
        out["Suites"] = str(suites)
    parking = g("parkingSpaces", "parking", "parkingSpots", "carSpaces")
    if parking is not None:
        out["Vagas"] = str(parking)

    addr = g("address", "location", "addressLocation", "place")
    if isinstance(addr, dict):
        parts = []
        for k in ("street", "streetAddress", "streetName", "address", "fullAddress"):
            v = addr.get(k) or addr.get(k.lower())
            if v:
                parts.append(str(v))
        for k in ("neighborhood", "city", "state"):
            v = addr.get(k)
            if v:
                parts.append(str(v))
        if parts:
            out["Endereço"] = ", ".join(parts)
    elif isinstance(addr, str):
        out["Endereço"] = addr

    if not out["Endereço"]:
        title = g("title", "headline", "name")
        if title:
            out["Endereço"] = str(title)

    for k,v in out.items():
        if isinstance(v, str):
            out[k] = re.sub(r"\s+", " ", v).strip()
    return out

# Fallback HTML heuristics (lightweight)
def gather_listing_links_generic(listings_page: str, max_links: int = 50, timeout: int = 20) -> List[str]:
    resp = safe_get(listings_page, timeout=timeout)
    if not resp:
        return []
    base = resp.url
    soup = BeautifulSoup(resp.text, "lxml")
    anchors = soup.find_all("a", href=True)
    links = []
    seen = set()
    keywords = ["imovel", "imóvel", "anuncio", "anúncio", "apartamento", "venda", "aluguel", "detalhes", "property"]
    for a in anchors:
        href = a["href"].strip()
        if href.startswith("#") or href.lower().startswith("javascript"):
            continue
        full = href if href.startswith("http") else (base.rstrip("/") + "/" + href.lstrip("/"))
        parsed = urlparse(full)
        if parsed.scheme not in ("http", "https"):
            continue
        if full in seen:
            continue
        anchor_text = (a.get_text(" ", strip=True) or "").lower()
        lower_href = href.lower()
        if any(k in anchor_text for k in keywords) or any(k in lower_href for k in keywords):
            links.append(full)
            seen.add(full)
        else:
            if re.search(r"/\d{3,}", parsed.path):
                links.append(full)
                seen.add(full)
        if len(links) >= max_links:
            break
    return links

def parse_listing_page_basic(link: str, timeout: int = 20) -> Dict[str,Any]:
    parsed = {"Endereço": None, "Valor": None, "Condominio": None, "IPTU": None, "M2": None, "Quartos": None, "Suites": None, "Vagas": None, "Link": link}
    resp = safe_get(link, timeout=timeout)
    if not resp:
        return parsed
    soup = BeautifulSoup(resp.text, "lxml")
    text = soup.get_text(separator="\n", strip=True)
    price_match = re.search(r"(R\$\s?[\d\.,]+)", text)
    if price_match:
        parsed["Valor"] = price_match.group(1)
    area_match = re.search(r"(\d{2,4}(?:[.,]\d{1,2})?)\s*(m2|m²)", text, re.I)
    if area_match:
        parsed["M2"] = area_match.group(1)
    q_match = re.search(r"(\d+)\s+quartos?", text, re.I)
    if q_match:
        parsed["Quartos"] = q_match.group(1)
    s_match = re.search(r"(\d+)\s+su[ií]tes?", text, re.I)
    if s_match:
        parsed["Suites"] = s_match.group(1)
    v_match = re.search(r"(\d+)\s+vagas?", text, re.I)
    if v_match:
        parsed["Vagas"] = v_match.group(1)
    addr_tag = soup.find("address")
    if addr_tag:
        parsed["Endereço"] = addr_tag.get_text(" ", strip=True)
    else:
        m = re.search(r"Endere[cç]o[:\s]*([^\n\r]+)", text, re.I)
        if m:
            parsed["Endereço"] = m.group(1).strip()
    return parsed

def df_to_excel_bytes(df: pd.DataFrame, sheet_name: str = "listings") -> bytes:
    bio = BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
    bio.seek(0)
    return bio.read()

# -------------------------
# UI
# -------------------------
st.title("🏠 Zap Imóveis — Extração via POST (API)")
st.markdown(
    "Cole a URL da página de listagens do Zap Imóveis (por ex.: resultados de busca). "
    "O app fará uma chamada POST para a API do Zap com filtros (cidade/bairro/área) e tentará retornar os anúncios."
)

# Read safe defaults from session settings
default_max_results = int(get_setting("max_results", 100))
default_page_size = int(get_setting("page_size", 50))
default_timeout = int(get_setting("timeout_sec", 20))

col_main, col_side = st.columns([3,1])
with col_main:
    listings_url = st.text_input("URL da página de listagens (Zap Imóveis)", value=st.session_state.get("last_listings_url",""))
    city = st.text_input("Cidade (opcional) — ex: São Paulo", value="")
    neighborhood = st.text_input("Bairro (opcional) — ex: Pinheiros", value="")
    area_min = st.number_input("Área mínima (m², opcional)", min_value=0, value=0, step=1)
    area_max = st.number_input("Área máxima (m², opcional)", min_value=0, value=0, step=1)
    max_results = st.number_input("Máx resultados (total)", min_value=10, max_value=1000, value=default_max_results)
    page_size = st.number_input("Tamanho da página (page size)", min_value=10, max_value=200, value=default_page_size)
with col_side:
    run_btn = st.button("Extrair via API POST")
    fallback_btn = st.button("Forçar heurística HTML")
    clear_btn = st.button("Limpar resultados")

if clear_btn:
    st.session_state.pop("listings_df", None)
    st.success("Resultados limpos.")

if run_btn:
    if not listings_url:
        st.error("Cole a URL da página de listagens antes de rodar.")
    else:
        st.session_state["last_listings_url"] = listings_url
        payload = build_payload_for_zap(city=city or None,
                                        neighborhood=neighborhood or None,
                                        area_min=(area_min if area_min>0 else None),
                                        area_max=(area_max if area_max>0 else None),
                                        page=1,
                                        size=int(page_size))
        st.info("Enviando requisição POST para glue-api.zapimoveis.com.br ...")
        with st.spinner("Chamando API do Zap (POST)..."):
            api_url = "https://glue-api.zapimoveis.com.br/v3/listings"
            resp = safe_post(api_url, json_payload=payload, timeout=default_timeout)
        results: List[Dict[str,Any]] = []
        if resp:
            try:
                j = resp.json()
                items = j.get("content") or j.get("listings") or j.get("data") or j.get("results") or j.get("hits") or j
                if isinstance(items, dict) and "content" in items:
                    items = items["content"]
                if isinstance(items, dict) and "listings" in items:
                    items = items["listings"]
                if isinstance(items, dict) and "data" in items and isinstance(items["data"], list):
                    items = items["data"]
                if isinstance(items, list) and items:
                    st.success(f"API retornou {len(items)} itens. Extraindo campos...")
                    max_take = min(int(max_results), len(items))
                    progress = st.progress(0)
                    for i, it in enumerate(items[:max_take], start=1):
                        try:
                            parsed = extract_listing_from_zap_item(it if isinstance(it, dict) else dict(it))
                        except Exception:
                            logger.exception("Erro extraindo item JSON")
                            parsed = {"Endereço": None, "Valor": None, "Condominio": None, "IPTU": None,
                                      "M2": None, "Quartos": None, "Suites": None, "Vagas": None, "Link": None}
                        results.append(parsed)
                        progress.progress(int(i/max_take*100))
                    progress.empty()
                else:
                    st.warning("A API respondeu, mas não retornou uma lista de anúncios no payload padrão.")
                    st.code(json.dumps(j, indent=2, ensure_ascii=False)[:5000])
            except Exception as e:
                logger.exception("Erro lendo JSON retornado pela API")
                st.error(f"Falha ao processar JSON da API: {e}")
        else:
            st.warning("A API não respondeu (resp é None) ou retornou erro. Irei tentar heurística de links HTML.")

        # fallback to heuristics if no results
        if not results:
            st.info("Tentando heurística genérica de links (HTML scraping)...")
            links = gather_listing_links_generic(listings_url, max_links=int(max_results), timeout=default_timeout)
            if not links:
                st.error("Nenhum link de anúncio encontrado com heurística — o site provavelmente carrega via JS. Podemos usar Playwright (renderização) se quiser.")
            else:
                st.info(f"{len(links)} links encontrados — extraindo páginas individuais.")
                progress = st.progress(0)
                for i, link in enumerate(links[:int(max_results)], start=1):
                    parsed = parse_listing_page_basic(link, timeout=default_timeout)
                    results.append(parsed)
                    progress.progress(int(i/len(links)*100))
                progress.empty()

        # Finalize DataFrame
        if results:
            df = pd.DataFrame(results)
            cols = ["Endereço","Valor","Condominio","IPTU","M2","Quartos","Suites","Vagas","Link"]
            for c in cols:
                if c not in df.columns:
                    df[c] = None
            df = df[cols]
            st.session_state["listings_df"] = df
            st.success(f"Extração finalizada: {len(df)} registros.")
        else:
            st.warning("Nenhum anúncio extraído.")

if fallback_btn:
    st.session_state["last_listings_url"] = st.session_state.get("last_listings_url","")
    url = st.session_state.get("last_listings_url","")
    if not url:
        st.error("Defina a URL antes de forçar heurística.")
    else:
        links = gather_listing_links_generic(url, max_links=int(get_setting("max_results", 100)), timeout=int(get_setting("timeout_sec", 20)))
        results = []
        if not links:
            st.error("Nenhum link encontrado com heurística.")
        else:
            progress = st.progress(0)
            for i, link in enumerate(links, start=1):
                parsed = parse_listing_page_basic(link, timeout=int(get_setting("timeout_sec", 20)))
                results.append(parsed)
                progress.progress(int(i/len(links)*100))
            progress.empty()
        if results:
            df = pd.DataFrame(results)
            cols = ["Endereço","Valor","Condominio","IPTU","M2","Quartos","Suites","Vagas","Link"]
            for c in cols:
                if c not in df.columns:
                    df[c] = None
            df = df[cols]
            st.session_state["listings_df"] = df
            st.success(f"Extração heurística concluída: {len(df)} registros.")

# Result display / download
if st.session_state.get("listings_df") is not None:
    df_out: pd.DataFrame = st.session_state["listings_df"]
    st.markdown("### Resultados (pré-visualização)")
    st.dataframe(df_out.head(200))

    excel_bytes = df_to_excel_bytes(df_out)
    ts_fname = datetime.now().strftime("%Y%m%d-%H%M%S")
    fname = f"anuncios_zap_post_{ts_fname}.xlsx"
    st.download_button("Baixar Excel com anúncios", data=excel_bytes, file_name=fname, mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

# Diagnostics / tips
with st.expander("Diagnóstico e dicas"):
    st.write("Dicas e diagnóstico:")
    st.write("- Caso a API não responda, cole aqui a URL completa da listagem e eu ajusto o payload (pode ser necessário incluir filtros adicionais).")
    st.write("- Se o Zap bloquear, podemos simular cabeçalhos extras; em último caso, usamos Playwright (renderização).")
    st.write("- Se quiser precisão máxima, envie 1 URL de anúncio e eu adapto os campos com seletores específicos.")
    st.write("")
    st.json({
        "last_listings_url": st.session_state.get("last_listings_url",""),
        "city": locals().get("city", ""),
        "neighborhood": locals().get("neighborhood", ""),
        "area_min": int(locals().get("area_min", 0)) if locals().get("area_min", 0) else None,
        "area_max": int(locals().get("area_max", 0)) if locals().get("area_max", 0) else None,
        "max_results": int(max_results) if "max_results" in locals() else get_setting("max_results", 100),
        "page_size": int(page_size) if "page_size" in locals() else get_setting("page_size", 50),
        "timeout_sec": int(get_setting("timeout_sec", 20))
    })

st.caption("Este app tenta uso API-first (POST) ao Glue API do Zap; caso não responda, usa heurística HTML como fallback. Se quiser que eu ajuste filtros ou headers, cole um exemplo de URL e eu adapto.")


