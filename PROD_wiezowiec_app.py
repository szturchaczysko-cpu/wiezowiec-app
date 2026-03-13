import streamlit as st
import vertexai
from vertexai.generative_models import GenerativeModel, Content, Part, SafetySetting, HarmCategory, HarmBlockThreshold
from google.oauth2 import service_account
from datetime import datetime
import json, re, pytz, time
import pandas as pd
import firebase_admin
from firebase_admin import credentials, firestore
import requests

# --- MODUŁ FORUM ---
try:
    from forum_module import execute_forum_actions, forum_read, discover_roots
    FORUM_ENABLED = True
except ImportError:
    FORUM_ENABLED = False

# --- KONFIGURACJA ---
st.set_page_config(page_title="Elektryczny Wieżowiec", layout="wide", page_icon="🏢")

if not firebase_admin._apps:
    creds_dict = json.loads(st.secrets["FIREBASE_CREDS"])
    creds = credentials.Certificate(creds_dict)
    firebase_admin.initialize_app(creds)
db = firestore.client()

# --- BRAMKA HASŁA ---
if "password_correct" not in st.session_state:
    st.session_state.password_correct = False

if not st.session_state.password_correct:
    st.header("🏢 Elektryczny Wieżowiec — Logowanie")
    pwd = st.text_input("Hasło admina:", type="password")
    if st.button("Zaloguj"):
        if pwd == st.secrets["ADMIN_PASSWORD"]:
            st.session_state.password_correct = True
            st.rerun()
        else:
            st.error("Błędne hasło")
    st.stop()

# --- PROJEKTY GCP ---
try:
    GCP_PROJECTS = list(st.secrets["GCP_PROJECT_IDS"])
except:
    GCP_PROJECTS = []
    st.error("🚨 Brak GCP_PROJECT_IDS w secrets!")

# --- PROMPTY WIEŻOWCA ---
WIEZOWIEC_PROMPT_URLS = {
    "Wieżowiec v5 (stabilny)": "https://raw.githubusercontent.com/szturchaczysko-cpu/szturchacz/refs/heads/main/prompt_wiezowiec_v4_gemini-3.md",
}
custom_data = (db.collection("admin_config").document("custom_prompts").get().to_dict() or {}).get("urls", {})
for name, url in custom_data.items():
    if "wiezowiec" in name.lower() or "wieżowiec" in name.lower() or "ew_" in name.lower():
        WIEZOWIEC_PROMPT_URLS[name] = url


@st.cache_data(ttl=3600)
def get_remote_prompt(url):
    try:
        r = requests.get(url)
        r.raise_for_status()
        return r.text
    except Exception as e:
        st.error(f"Błąd pobierania promptu: {e}")
        return ""


# ==========================================
# FIRESTORE: ZARZĄDZANIE WSADAMI
# ==========================================
# Kolekcja: ew_wsady
# Dokumenty: "swinka", "uszki", "szturchacz"
# Pole: "data" = tekst wsadu, "updated_at" = timestamp

WSADY_COLLECTION = "ew_wsady"

def load_wsad(name):
    """Pobierz wsad z bazy"""
    doc = db.collection(WSADY_COLLECTION).document(name).get()
    if doc.exists:
        return doc.to_dict().get("data", "")
    return ""

def save_wsad(name, data):
    """Zapisz wsad (nadpisz)"""
    db.collection(WSADY_COLLECTION).document(name).set({
        "data": data,
        "updated_at": firestore.SERVER_TIMESTAMP,
    })

def clear_all_wsady():
    """Wyczyść wszystkie wsady"""
    for name in ["swinka", "uszki", "szturchacz"]:
        db.collection(WSADY_COLLECTION).document(name).delete()

def parse_szturchacz_blocks(text):
    """Dzieli tekst szturchacza na bloki per zamówienie (NrZam → tekst bloku).
    
    Rozpoznaje formaty:
    - NrZam: 366000 (z prefiksem)
    - ZN366000 (z prefiksem ZN)
    - 366000 (gołe 6+ cyfrowe numery na początku linii — format tabeli)
    """
    if not text or not text.strip():
        return {}
    
    blocks = {}
    lines = text.split('\n')
    current_block = []
    current_nr = None
    
    for line in lines:
        stripped = line.strip()
        
        # Szukaj NrZam w różnych formatach
        nr_match = None
        
        # Format 1: NrZam: XXXXX lub NrZam XXXXX
        nr_match = re.search(r'NrZam[:\s]+(\S+)', line, re.IGNORECASE)
        
        # Format 2: ZN + cyfry
        if not nr_match:
            nr_match = re.match(r'^(ZN\d+)', stripped)
        
        # Format 3: gołe 5-7 cyfrowe numery na początku linii (format tabeli szturchacza)
        # Nie łap numerów listów przewozowych (13+ cyfr) ani dat (8 cyfr z myślnikami)
        if not nr_match:
            nr_match = re.match(r'^(\d{5,7})\s', stripped)
        
        if nr_match:
            # Zapisz poprzedni blok
            if current_nr and current_block:
                blocks[current_nr] = '\n'.join(current_block)
            # Rozpocznij nowy blok
            candidate = nr_match.group(1).strip().rstrip(',').rstrip('|')
            # Filtruj fałszywe matche (nagłówki tabeli itp.)
            if candidate.lower() in ('data', 'zama', 'nr', 'nrzam', 'mail', 'tel', 'kraj'):
                current_block.append(line) if current_block is not None else None
            else:
                current_nr = candidate
                current_block = [line]
        else:
            if current_block is not None:
                current_block.append(line)
    
    # Zapisz ostatni blok
    if current_nr and current_block:
        blocks[current_nr] = '\n'.join(current_block)
    
    # Jeśli parser nie znalazł bloków, zwróć cały tekst jako jeden blok
    if not blocks and text.strip():
        blocks["_RAW_"] = text.strip()
    
    return blocks

def merge_szturchacz(existing_text, new_text):
    """
    Dopełnij istniejący wsad szturchacza nowymi zamówieniami.
    Jeśli zamówienie o tym samym NrZam istnieje — nadpisz nowszą wersją.
    Jeśli nie istnieje — dodaj.
    """
    existing_blocks = parse_szturchacz_blocks(existing_text)
    new_blocks = parse_szturchacz_blocks(new_text)
    
    # Merge: nowe nadpisują istniejące, reszta pozostaje
    merged = {**existing_blocks, **new_blocks}
    
    added = len([k for k in new_blocks if k not in existing_blocks])
    updated = len([k for k in new_blocks if k in existing_blocks])
    
    # Złóż z powrotem w tekst
    merged_text = '\n\n'.join(merged.values())
    
    return merged_text, added, updated, len(merged)

def count_lines(text):
    """Policz ile zamówień (bloków) jest w tekście"""
    if not text or not text.strip():
        return 0
    blocks = parse_szturchacz_blocks(text)
    # Nie licz klucza _RAW_ jako zamówienia
    count = len([k for k in blocks if k != "_RAW_"])
    return max(count, 1 if text.strip() and count == 0 else 0)


# ==========================================
# PARSER WYJŚCIA WIEŻOWCA (bez zmian)
# ==========================================
def parse_wiezowiec_output(text):
    cases = []
    current_grupa = None
    grupa_patterns = {
        "DE": r'▬+\s*OPERATORZY\s+DE',
        "FR": r'▬+\s*OPERATORZY\s+FR',
        "UKPL": r'▬+\s*OPERATORZY\s+UKPL',
    }
    lines = text.split('\n')
    i = 0
    while i < len(lines):
        line = lines[i].strip()
        for grupa, pattern in grupa_patterns.items():
            if re.search(pattern, line):
                current_grupa = grupa
                break
        # Nagłówek: [SCORE=XXX] ikona | ...
        score_match = re.match(r'^\[SCORE=(\d+)\]\s*([🔴🟡⚪📦])\s*\|\s*(.*)', line)
        if not score_match:
            # Alternatywny format: ikona [score] | ...
            score_match = re.match(r'^([🔴🟡⚪📦])\s*\[(\d+)\]\s*\|\s*(.*)', line)
            if score_match:
                icon = score_match.group(1)
                score = int(score_match.group(2))
                label = score_match.group(3).strip()
            else:
                score_match = None
        else:
            score = int(score_match.group(1))
            icon = score_match.group(2)
            label = score_match.group(3).strip()
        
        if score_match and current_grupa:
            naglowek = line
            i += 1
            blok_lines = []
            # Zbierz linie: punktacja + pełna linia szturchacza
            while i < len(lines):
                nl = lines[i].strip()
                if nl == '---' or nl.startswith('▬') or nl.startswith('═══'):
                    break
                if re.match(r'^\[SCORE=\d+\]', nl) or re.match(r'^[🔴🟡⚪📦]\s*\[\d+\]', nl):
                    break
                if nl:
                    blok_lines.append(lines[i])
                i += 1
            
            pelna_linia = '\n'.join(blok_lines).strip()
            
            # Wyciągnij numer zamówienia
            numer = None
            for p in [r'NrZam[:\s]+(\S+)', r'Nr\s*Zam[:\s]+(\S+)', r'(ZN\d+)', r'(ZW\d+[/]\d+)']:
                m = re.search(p, pelna_linia, re.IGNORECASE)
                if m:
                    numer = m.group(1).strip().rstrip(',').rstrip('|')
                    break
            
            # Fallback: szukaj gołego 5-7 cyfrowego numeru na początku linii (format tabeli)
            if not numer:
                for bl in blok_lines:
                    m = re.match(r'^\s*(\d{5,7})\s', bl)
                    if m:
                        numer = m.group(1)
                        break
            
            # Fallback 2: szukaj gołego numeru gdziekolwiek w nagłówku lub label
            if not numer:
                for src in [naglowek, label]:
                    m = re.search(r'(\d{5,7})', src)
                    if m:
                        numer = m.group(1)
                        break
            
            idx_m = re.search(r'Index:\s*(\S+)', label)
            index_handlowy = idx_m.group(1) if idx_m else ""
            if not index_handlowy:
                lindx_m = re.search(r'lindexy[:\s]+(\S+)', pelna_linia, re.IGNORECASE)
                if lindx_m:
                    index_handlowy = lindx_m.group(1)
            
            if pelna_linia and numer:
                cases.append({
                    "numer_zamowienia": numer,
                    "score": score,
                    "priority_icon": icon,
                    "priority_label": label,
                    "grupa": current_grupa,
                    "index_handlowy": index_handlowy,
                    "pelna_linia_szturchacza": pelna_linia,
                    "naglowek_priorytetowy": naglowek,
                })
            continue
        
        if 'ALERT' in line and 'BRAK W SZTURCHACZU' in line:
            i += 1
            while i < len(lines) and not lines[i].strip().startswith('═══'):
                i += 1
            continue
        i += 1
    return cases


# ==========================================
# GŁÓWNY INTERFEJS
# ==========================================
st.title("🏢 Elektryczny Wieżowiec")
st.caption("System zarządzania priorytetami — wsady z pamięcią")

# --- Funkcje autopilota (globalne — używane przez oba taby) ---
AUTOPILOT_DOC = db.collection("autopilot_config").document("status")

def get_autopilot_status():
    doc = AUTOPILOT_DOC.get()
    if doc.exists:
        return doc.to_dict()
    return {"state": "idle", "processed": 0, "total": 0, "current_nrzam": "", "last_error": ""}

def set_autopilot_status(data):
    AUTOPILOT_DOC.set(data, merge=True)

GRUPA_MAP_GLOBAL = {"DE": "Operatorzy_DE", "FR": "Operatorzy_FR", "UKPL": "Operatorzy_UK/PL"}

def build_autopilot_queue(percent, obsada, ap_work_date_str):
    """Buduje kolejkę autopilota: top X% casów globalnie po score, round-robin per grupa."""
    all_wolne_docs = db.collection("ew_cases").where("status", "==", "wolny").get()
    wolne = []
    for cdoc in all_wolne_docs:
        cdata = cdoc.to_dict()
        cdata["_doc_id"] = cdoc.id
        if cdata.get("autopilot_status") != "calculated":
            g = cdata.get("grupa", "")
            if g in obsada:  # tylko grupy z obsadą
                wolne.append(cdata)
    
    # Sortuj GLOBALNIE po score (mieszaj grupy)
    wolne.sort(key=lambda c: -c.get("score", 0))
    
    # Weź top X%
    count = max(1, int(len(wolne) * percent / 100)) if wolne else 0
    top_cases = wolne[:count]
    
    # Round-robin per grupa
    group_counters = {g: 0 for g in obsada}
    case_queue = []
    
    for wc in top_cases:
        g = wc.get("grupa", "")
        if g not in obsada or not obsada[g]:
            continue
        ops = obsada[g]
        assigned_op = ops[group_counters[g] % len(ops)]
        group_counters[g] += 1
        case_queue.append({
            "doc_id": wc["_doc_id"],
            "nrzam": wc.get("numer_zamowienia", "?"),
            "operator": assigned_op,
            "grupa": g,
            "grupa_operatorska": GRUPA_MAP_GLOBAL.get(g, "Operatorzy_DE"),
        })
        db.collection("ew_cases").document(wc["_doc_id"]).update({
            "autopilot_assigned_to": assigned_op,
        })
    
    return case_queue, len(wolne)

tab_wsady, tab_generuj, tab_autopilot, tab_batches, tab_cases, tab_skipped = st.tabs([
    "📂 Wsady",
    "⚡ Generuj + Autopilot",
    "🤖 Dolewka + Status",
    "📦 Historia partii",
    "📋 Przegląd casów",
    "⏭️ Pominięte (archiwum)"
])


# ==========================================
# 📂 ZAKŁADKA: WSADY
# ==========================================
with tab_wsady:
    st.subheader("📂 Zarządzanie wsadami")
    st.markdown("**Świnka / Uszki** → nowy plik NADPISUJE poprzedni  \n"
                "**Szturchacz** → nowy plik DOPEŁNIA istniejącą pulę (to samo NrZam = aktualizacja)")
    
    # Pokaż aktualny stan
    st.markdown("---")
    st.markdown("### 📊 Aktualny stan wsadów w bazie")
    
    cur_swinka = load_wsad("swinka")
    cur_uszki = load_wsad("uszki")
    cur_szturchacz = load_wsad("szturchacz")
    
    cs1, cs2, cs3 = st.columns(3)
    with cs1:
        n_sw = count_lines(cur_swinka)
        st.metric("🐷 Świnka", f"{n_sw} zamówień" if cur_swinka else "Brak")
    with cs2:
        st.metric("📦 Uszki", "Załadowane" if cur_uszki else "Brak")
    with cs3:
        n_sz = count_lines(cur_szturchacz)
        st.metric("📋 Szturchacz (pula)", f"{n_sz} zamówień" if cur_szturchacz else "Brak")
    
    st.markdown("---")
    
    # --- ŁADOWANIE WSADÓW ---
    st.markdown("### ⬆️ Załaduj wsady")
    
    col_w1, col_w2, col_w3 = st.columns(3)
    
    with col_w1:
        st.markdown("**🐷 ŚWINKA** (nadpisuje)")
        wsad_swinka = st.text_area("Wklej świnkę:", height=250, key="input_swinka")
        if st.button("💾 Załaduj świnkę", key="btn_swinka"):
            if wsad_swinka.strip():
                save_wsad("swinka", wsad_swinka.strip())
                st.success(f"✅ Świnka załadowana ({count_lines(wsad_swinka)} zamówień). Poprzednia nadpisana.")
                st.rerun()
            else:
                st.error("Pole jest puste!")
    
    with col_w2:
        st.markdown("**📦 USZKI** (nadpisuje)")
        wsad_uszki = st.text_area("Wklej uszki:", height=250, key="input_uszki")
        if st.button("💾 Załaduj uszki", key="btn_uszki"):
            if wsad_uszki.strip():
                save_wsad("uszki", wsad_uszki.strip())
                st.success("✅ Uszki załadowane. Poprzednie nadpisane.")
                st.rerun()
            else:
                st.error("Pole jest puste!")
    
    with col_w3:
        st.markdown("**📋 SZTURCHACZ** (dopełnia pulę)")
        wsad_szturchacz = st.text_area("Wklej szturchacza:", height=250, key="input_szturchacz")
        if st.button("💾 Załaduj szturchacza (dopełnij)", key="btn_szturchacz"):
            if wsad_szturchacz.strip():
                existing = load_wsad("szturchacz")
                merged, added, updated, total = merge_szturchacz(existing, wsad_szturchacz.strip())
                save_wsad("szturchacz", merged)
                st.success(f"✅ Szturchacz dopełniony — dodano {added} nowych, "
                           f"zaktualizowano {updated} istniejących. Pula razem: {total} zamówień.")
                st.rerun()
            else:
                st.error("Pole jest puste!")
    
    st.markdown("---")
    
    # --- CZYSZCZENIE ---
    st.markdown("### 🗑️ Czyszczenie")
    col_clr1, col_clr2 = st.columns(2)
    with col_clr1:
        if st.button("🗑️ Wyczyść WSZYSTKIE wsady", type="primary"):
            clear_all_wsady()
            st.success("🗑️ Wszystkie wsady wyczyszczone (świnka + uszki + szturchacz).")
            st.rerun()
    with col_clr2:
        if st.button("🗑️ Wyczyść kolejkę casów (ew_cases)"):
            # Pobierz WSZYSTKIE casy z bazy (nie po batch_id)
            all_ew = db.collection("ew_cases").limit(5000).get()
            deleted = 0
            archived = 0
            for c in all_ew:
                cdata = c.to_dict()
                # Case z nienaprawionym komentarzem → archiwizuj
                if cdata.get("skip_reason") and not cdata.get("skip_fixed"):
                    cdata["archived_at"] = firestore.SERVER_TIMESTAMP
                    cdata["archived_from_batch"] = cdata.get("batch_id", "unknown")
                    db.collection("ew_cases_archived").document(c.id).set(cdata)
                    archived += 1
                db.collection("ew_cases").document(c.id).delete()
                deleted += 1
            # Wyczyść też wszystkie batche
            all_batches = db.collection("ew_batches").get()
            for bdoc in all_batches:
                db.collection("ew_batches").document(bdoc.id).delete()
            msg = f"🗑️ Usunięto {deleted} casów i {len(all_batches)} batchy. Czysta baza."
            if archived > 0:
                msg += f" ⏭️ {archived} pominiętych (nienaprawionych) przeniesiono do archiwum."
            st.success(msg)
            st.rerun()
    
    # Podgląd
    st.markdown("---")
    with st.expander("👀 Podgląd aktualnej puli szturchacza"):
        if cur_szturchacz:
            st.text(cur_szturchacz[:5000] + ("\n\n... (obcięto podgląd)" if len(cur_szturchacz) > 5000 else ""))
        else:
            st.info("Pula szturchacza jest pusta.")


# ==========================================
# ⚡ ZAKŁADKA: GENERUJ RAPORT
# ==========================================
with tab_generuj:
    st.subheader("⚡ Generuj raport priorytetów")
    st.caption("Używa aktualnie załadowanych wsadów z zakładki Wsady")
    
    # Sprawdź co jest załadowane
    cur_swinka = load_wsad("swinka")
    cur_uszki = load_wsad("uszki")
    cur_szturchacz = load_wsad("szturchacz")
    
    s1, s2, s3 = st.columns(3)
    with s1:
        st.metric("🐷 Świnka", "✅" if cur_swinka else "❌ Brak")
    with s2:
        st.metric("📦 Uszki", "✅" if cur_uszki else "⚠️ Opcjonalnie")
    with s3:
        st.metric("📋 Szturchacz", f"✅ ({count_lines(cur_szturchacz)})" if cur_szturchacz else "❌ Brak")
    
    if not cur_swinka or not cur_szturchacz:
        st.warning("⚠️ Potrzebujesz minimum świnki i szturchacza. Załaduj wsady w zakładce 📂 Wsady.")
        st.stop()
    
    st.markdown("---")
    
    col1, col2 = st.columns(2)
    with col1:
        sel_prompt = st.selectbox("Prompt Wieżowca:", list(WIEZOWIEC_PROMPT_URLS.keys()))
        sel_prompt_url = WIEZOWIEC_PROMPT_URLS[sel_prompt]
    with col2:
        if GCP_PROJECTS:
            proj_opts = [f"{i+1} - {p}" for i, p in enumerate(GCP_PROJECTS)]
            sel_proj = st.selectbox("Projekt GCP:", proj_opts)
            proj_idx = int(sel_proj.split(" - ")[0]) - 1
            current_project = GCP_PROJECTS[proj_idx]
        else:
            current_project = ""
        model_choice = st.selectbox("Model AI:", ["gemini-2.5-pro", "gemini-2.5-flash"])
    
    # --- DATA OBRÓBKI (obowiązkowa) ---
    data_obrobki = st.date_input("📅 Data obróbki (kiedy operatorzy będą obrabiać te casy):", value=None, key="data_obrobki")
    if data_obrobki:
        st.success(f"📅 Data obróbki: **{data_obrobki.strftime('%d.%m.%Y')}** — prompt potraktuje tę datę jako 'dziś'.")
    else:
        st.warning("⚠️ Wybierz datę obróbki żeby rozpocząć przeliczanie.")
    
    st.markdown("---")
    
    # ==========================================
    # 👥 OBSADA + AUTOPILOT (wspólne parametry)
    # ==========================================
    st.markdown("### 👥 Obsada operatorów + Autopilot")
    st.caption("Wybierz operatorów per grupa. Po wygenerowaniu raportu autopilot automatycznie przelicza X% casów.")
    
    ALL_OPERATORS_LIST = ["Emilia", "Oliwia", "Magda", "Ewelina", "Iwona", "Marlena", "Sylwia", "EwelinaG", "Andrzej", "Marta", "Klaudia", "Kasia", "Romana"]
    GRUPA_MAP = {"DE": "Operatorzy_DE", "FR": "Operatorzy_FR", "UKPL": "Operatorzy_UK/PL"}
    ROLE_TO_GRUPA = {"Operatorzy_DE": "DE", "Operatorzy_FR": "FR", "Operatorzy_UK/PL": "UKPL"}
    
    ops_by_grupa = {"DE": [], "FR": [], "UKPL": []}
    for op_name_candidate in ALL_OPERATORS_LIST:
        try:
            cfg_doc = db.collection("operator_configs").document(op_name_candidate).get()
            if cfg_doc.exists:
                role = cfg_doc.to_dict().get("role", "Operatorzy_DE")
                grupa = ROLE_TO_GRUPA.get(role, "DE")
                ops_by_grupa[grupa].append(op_name_candidate)
            else:
                ops_by_grupa["DE"].append(op_name_candidate)
        except Exception:
            ops_by_grupa["DE"].append(op_name_candidate)
    
    col_obs1, col_obs2, col_obs3 = st.columns(3)
    with col_obs1:
        st.markdown("**🇩🇪 DE**")
        gen_ops_de = st.multiselect("Operatorzy DE:", ops_by_grupa["DE"], key="gen_ops_de")
    with col_obs2:
        st.markdown("**🇫🇷 FR**")
        gen_ops_fr = st.multiselect("Operatorzy FR:", ops_by_grupa["FR"], key="gen_ops_fr")
    with col_obs3:
        st.markdown("**🇬🇧 UKPL**")
        gen_ops_ukpl = st.multiselect("Operatorzy UKPL:", ops_by_grupa["UKPL"], key="gen_ops_ukpl")
    
    gen_obsada = {}
    if gen_ops_de: gen_obsada["DE"] = gen_ops_de
    if gen_ops_fr: gen_obsada["FR"] = gen_ops_fr
    if gen_ops_ukpl: gen_obsada["UKPL"] = gen_ops_ukpl
    
    if gen_obsada:
        summary_parts = [f"{g}: {', '.join(ops)} ({len(ops)} os.)" for g, ops in gen_obsada.items()]
        st.success(f"📋 Obsada: {' | '.join(summary_parts)}")
    
    # Procent autopilota
    col_pct1, col_pct2 = st.columns(2)
    with col_pct1:
        autopilot_pct = st.slider("🤖 % casów do przeliczenia autopilotem:", min_value=0, max_value=100, value=30, step=5, key="autopilot_pct")
    with col_pct2:
        st.caption(f"Po raporcie autopilot przelicza **{autopilot_pct}%** najwyżej punktowanych casów (globalnie po score, mieszając grupy).")
    
    # Zaawansowane parametry autopilota
    with st.expander("⚙️ Parametry autopilota"):
        PROMPT_URLS_hardcoded = {
            "Prompt Stabilny (prompt4624)": "https://raw.githubusercontent.com/szturchaczysko-cpu/szturchacz/refs/heads/main/prompt4624.txt",
        }
        custom_prompts_data = (db.collection("admin_config").document("custom_prompts").get().to_dict() or {}).get("urls", {})
        ALL_OP_PROMPT_URLS = {**PROMPT_URLS_hardcoded, **custom_prompts_data}
        
        col_ap1, col_ap2, col_ap3 = st.columns(3)
        with col_ap1:
            ap_prompt_name = st.selectbox("Prompt operatorski:", list(ALL_OP_PROMPT_URLS.keys()), key="gen_ap_prompt")
            ap_prompt_url = ALL_OP_PROMPT_URLS[ap_prompt_name]
        with col_ap2:
            ap_pause = st.slider("⏱️ Pauza (sek):", min_value=5, max_value=120, value=30, step=5, key="gen_ap_pause")
            ap_model = st.selectbox("Model AI (autopilot):", ["gemini-2.5-pro", "gemini-2.5-flash"], key="gen_ap_model")
        with col_ap3:
            available_keys = [f"{i+1} - {p}" for i, p in enumerate(GCP_PROJECTS)]
            ap_keys = st.multiselect("🔑 Klucze do rotacji:", available_keys, default=available_keys, key="gen_ap_keys")
            ap_key_indices = [int(k.split(" - ")[0]) - 1 for k in ap_keys]
    
    # Zapisz do session_state żeby dolewka mogła czytać
    st.session_state["_gen_obsada"] = gen_obsada
    st.session_state["_gen_ap_prompt_name"] = ap_prompt_name
    st.session_state["_gen_ap_prompt_url"] = ap_prompt_url
    st.session_state["_gen_ap_pause"] = ap_pause
    st.session_state["_gen_ap_model"] = ap_model
    st.session_state["_gen_ap_key_indices"] = ap_key_indices
    st.session_state["_gen_data_obrobki"] = data_obrobki
    
    st.markdown("---")
    
    # --- PRZYGOTOWANIE PARTII (analiza bez przeliczania) ---
    if st.button("📊 Przygotuj partycje (bez przeliczania)", type="secondary"):
        if not data_obrobki:
            st.error("⚠️ Wybierz datę obróbki!")
            st.stop()
        if not current_project:
            st.error("Brak projektu GCP!")
            st.stop()
        
        WIEZOWIEC_PROMPT = get_remote_prompt(sel_prompt_url)
        if not WIEZOWIEC_PROMPT:
            st.error("Nie udało się pobrać promptu!")
            st.stop()
        
        tz_pl = pytz.timezone('Europe/Warsaw')
        now = datetime.now(tz_pl)
        
        # --- TRYB INKREMENTALNY: sprawdź istniejące casy w bazie ---
        existing_docs = db.collection("ew_cases").limit(5000).get()
        existing_cases_map = {}  # NrZam → {status, score, priority_icon, priority_label, naglowek, grupa, ...}
        for edoc in existing_docs:
            ed = edoc.to_dict()
            enr = ed.get("numer_zamowienia", "")
            if enr:
                # Priorytet: w_toku > przydzielony > zakonczony > wolny
                prio_map = {"w_toku": 4, "przydzielony": 3, "zakonczony": 2, "wolny": 1}
                if enr in existing_cases_map:
                    if prio_map.get(ed.get("status"), 0) > prio_map.get(existing_cases_map[enr].get("status"), 0):
                        existing_cases_map[enr] = ed
                else:
                    existing_cases_map[enr] = ed
        
        # Rozdziel NrZamy z puli szturchacza na kategorie
        # Używamy tego samego parsera co merge_szturchacz
        szturchacz_blocks = parse_szturchacz_blocks(cur_szturchacz)
        szturchacz_nrzams = set(szturchacz_blocks.keys())
        # Usuń klucz _RAW_ jeśli parser nie rozpoznał bloków
        szturchacz_nrzams.discard("_RAW_")
        
        # Kategorie:
        # DO_PRZELICZENIA: nowe (nie ma w bazie) + zakończone (mogły się zmienić) + wspólne-zakończone
        # GOTOWE: wolne z bazy (score się nie zmienił) + przydzielone + w_toku
        nrzam_do_przeliczenia = set()
        nrzam_gotowe = {}  # NrZam → dane z bazy
        
        for nrzam in szturchacz_nrzams:
            if nrzam not in existing_cases_map:
                # Nowy case — nie było go w bazie
                nrzam_do_przeliczenia.add(nrzam)
            else:
                status = existing_cases_map[nrzam].get("status", "wolny")
                if status == "zakonczony":
                    # Zakończony — przelicz od nowa (operator mógł zmienić dane)
                    nrzam_do_przeliczenia.add(nrzam)
                else:
                    # Wolny / przydzielony / w_toku — gotowy wynik, nie przeliczaj
                    nrzam_gotowe[nrzam] = existing_cases_map[nrzam]
        
        # Dodaj też zakończone z bazy, które NIE są w aktualnym szturchaczu
        # (były w starym wsadzie, operator je zakończył — AI musi je widzieć)
        for nrzam, edata in existing_cases_map.items():
            if nrzam not in szturchacz_nrzams and edata.get("status") == "zakonczony":
                nrzam_do_przeliczenia.add(nrzam)
        
        is_incremental = len(nrzam_gotowe) > 0
        
        # Debug: pokaż co parser znalazł
        with st.expander(f"🔍 Debug: parser znalazł {len(szturchacz_nrzams)} NrZam w puli szturchacza", expanded=False):
            if szturchacz_nrzams:
                st.text(f"NrZamy ({len(szturchacz_nrzams)}): {', '.join(sorted(list(szturchacz_nrzams))[:30])}")
                if len(szturchacz_nrzams) > 30:
                    st.text(f"...+{len(szturchacz_nrzams)-30} więcej")
            else:
                st.warning("⚠️ Parser nie znalazł żadnych NrZam! Sprawdź format wsadu szturchacza.")
                st.text(f"Pierwsze 500 znaków puli:\n{cur_szturchacz[:500]}")
            
            if existing_cases_map:
                st.text(f"\nCasy w bazie ({len(existing_cases_map)}): {', '.join(sorted(list(existing_cases_map.keys()))[:30])}")
            else:
                st.text("\nBrak casów w bazie (pierwszy wsad).")
            
            st.text(f"\nDo przeliczenia: {len(nrzam_do_przeliczenia)}")
            st.text(f"Gotowe (z bazy): {len(nrzam_gotowe)}")
        
        # Wyświetl info o trybie
        if is_incremental:
            st.info(
                f"🔄 **Tryb inkrementalny:**\n"
                f"- **{len(nrzam_do_przeliczenia)}** zamówień do przeliczenia (nowe + zakończone)\n"
                f"- **{len(nrzam_gotowe)}** zamówień z gotowym wynikiem (wolne/przydzielone/w toku)"
            )
        else:
            st.info(f"🆕 **Pierwszy wsad:** {len(szturchacz_nrzams)} zamówień do przeliczenia od zera.")
        
        # --- Buduj partie zamówień do przeliczenia ---
        BATCH_SIZE = 60  # max zamówień na jedno wywołanie AI
        
        # Zbierz bloki szturchacza do przeliczenia
        nowe_szturchacz_parts = []
        nrzam_order = []  # zachowaj kolejność
        for nrzam in nrzam_do_przeliczenia:
            block = None
            if nrzam in szturchacz_blocks:
                block = szturchacz_blocks[nrzam]
            elif nrzam in existing_cases_map:
                saved_line = existing_cases_map[nrzam].get("pelna_linia_szturchacza", "")
                if saved_line:
                    block = saved_line
            if block:
                nowe_szturchacz_parts.append((nrzam, block))
                nrzam_order.append(nrzam)
        
        # Podziel na partie
        batches_to_process = []
        for i in range(0, len(nowe_szturchacz_parts), BATCH_SIZE):
            batch_chunk = nowe_szturchacz_parts[i:i+BATCH_SIZE]
            batches_to_process.append(batch_chunk)
        
        total_batches = len(batches_to_process)
        if total_batches == 0 and not nrzam_gotowe:
            st.warning("⚠️ Brak zamówień do przeliczenia.")
            st.stop()
        
        # Zapisz przygotowane partycje do session_state
        old_total = len(st.session_state.get("_ew_batches_to_process", []))
        st.session_state["_ew_batches_to_process"] = batches_to_process
        # Resetuj postęp tylko jeśli partycje się zmieniły (inny wsad)
        if total_batches != old_total:
            st.session_state["_ew_batches_done"] = 0
            st.session_state["_ew_all_cases"] = []
            st.session_state["_ew_all_raw_outputs"] = []
        st.session_state["_ew_nrzam_gotowe"] = nrzam_gotowe if is_incremental else {}
        st.session_state["_ew_is_incremental"] = is_incremental
        st.session_state["_ew_prompt_name"] = sel_prompt
        st.session_state["_ew_model"] = model_choice
        st.session_state["_ew_prompt_url"] = sel_prompt_url
        st.session_state["_ew_project"] = current_project
        
        batches_done = st.session_state.get("_ew_batches_done", 0)
        st.success(f"📦 **{total_batches} partii** (po ~{BATCH_SIZE} zamówień). "
                   f"{len(nowe_szturchacz_parts)} do przeliczenia"
                   + (f", {len(nrzam_gotowe)} już w bazie (gotowe)" if nrzam_gotowe else "")
                   + (f". **{batches_done} partii już przeliczonych** — kontynuuj od partii {batches_done+1}." if batches_done > 0 else "")
                   + ".")
        st.rerun()
    
    # --- PANEL PRZELICZANIA PARTII ---
    batches_to_process = st.session_state.get("_ew_batches_to_process", [])
    def _save_cases_to_db(batch_cases, batch_num, total_batches):
        """Zapisz casy z jednej paczki do bazy natychmiast."""
        
        # === W1: KOMPRESJA ZABLOKOWANYCH KLIENTÓW ===
        # Jeśli case ma "Zablokowany klient" w danych, grupuj po emailu.
        # Z grupy bierz tylko jeden (najwyższy score), resztę oznacz jako zablokowane.
        def extract_email(text):
            """Wyciągnij email z pelna_linia_szturchacza"""
            m = re.search(r'[\w.+-]+@[\w.-]+\.\w+', text)
            return m.group(0).lower() if m else None
        
        blocked_by_email = {}  # email -> [cases]
        normal_cases = []
        
        for case in batch_cases:
            linia = case.get("pelna_linia_szturchacza", "")
            if "zablokowany klient" in linia.lower() or "Zablokowany klient" in linia:
                email = extract_email(linia)
                if email:
                    if email not in blocked_by_email:
                        blocked_by_email[email] = []
                    blocked_by_email[email].append(case)
                else:
                    normal_cases.append(case)
            else:
                normal_cases.append(case)
        
        # Z każdej grupy zablokowanych bierz tylko najwyższy score
        compressed_count = 0
        for email, cases_group in blocked_by_email.items():
            cases_group.sort(key=lambda c: c.get("score", 0), reverse=True)
            normal_cases.append(cases_group[0])  # najwyższy score
            compressed_count += len(cases_group) - 1
        
        if compressed_count > 0:
            st.toast(f"🔗 Skompresowano {compressed_count} casów zablokowanych klientów (po emailu)")
        
        batch_cases = normal_cases
        # === KONIEC W1 ===
        
        # === W2: KOREKTA GRUPY PO KRAJU ===
        DE_COUNTRIES = {"germany", "austria", "switzerland", "liechtenstein"}
        FR_COUNTRIES = {"france", "belgium", "spain", "italy"}
        # UKPL = cała reszta (Luxembourg, Portugal, Sweden, Netherlands, Poland, UK, itd.)
        
        def detect_country_grupa(text):
            """Wykryj kraj z pelna_linia_szturchacza i zwróć poprawną grupę.
            DE/FR — jawna lista. Każdy inny wykryty kraj → UKPL. Brak kraju → None."""
            text_lower = text.lower()
            # Szukaj DE
            for country in DE_COUNTRIES:
                if country in text_lower:
                    return "DE"
            # Szukaj FR
            for country in FR_COUNTRIES:
                if country in text_lower:
                    return "FR"
            # Szukaj znanych krajów → UKPL
            known_countries = [
                "luxembourg", "poland", "portugal", "netherlands", "sweden", "denmark",
                "finland", "norway", "ireland", "united kingdom", "uk", "england",
                "czech", "slovakia", "hungary", "romania", "bulgaria", "croatia",
                "slovenia", "greece", "turkey", "serbia", "estonia", "latvia",
                "lithuania", "malta", "cyprus", "scotland", "wales",
            ]
            for country in known_countries:
                if country in text_lower:
                    return "UKPL"
            # Nie wykryto żadnego kraju
            return None
        
        corrected = 0
        no_country = 0
        for case in batch_cases:
            linia = case.get("pelna_linia_szturchacza", "")
            detected = detect_country_grupa(linia)
            if detected:
                if detected != case.get("grupa") or not case.get("grupa"):
                    case["grupa"] = detected
                    corrected += 1
            elif not case.get("grupa"):
                no_country += 1
        
        if corrected > 0:
            st.toast(f"🌍 Skorygowano/przypisano grupę dla {corrected} casów (po kraju)")
        if no_country > 0:
            st.toast(f"⚠️ {no_country} casów bez rozpoznanego kraju — brak grupy!")
        # === KONIEC W2 ===
        tz_pl = pytz.timezone('Europe/Warsaw')
        now = datetime.now(tz_pl)
        batch_id = f"batch_{now.strftime('%Y%m%d_%H%M%S')}_p{batch_num}"
        
        # Pobierz istniejące casy — zbierz WSZYSTKIE doc_id per NrZam (nie tylko jeden)
        existing_cases_docs = db.collection("ew_cases").limit(5000).get()
        existing_by_nrzam = {}  # NrZam → [{"doc_id": ..., "status": ...}, ...]
        for edoc in existing_cases_docs:
            edata = edoc.to_dict()
            enr = edata.get("numer_zamowienia", "")
            if enr:
                if enr not in existing_by_nrzam:
                    existing_by_nrzam[enr] = []
                existing_by_nrzam[enr].append({"doc_id": edoc.id, "status": edata.get("status", "wolny")})
        
        saved = 0
        skipped = 0
        deleted = 0
        
        for i, case in enumerate(batch_cases):
            nrzam = case.get("numer_zamowienia", "")
            existing_list = existing_by_nrzam.get(nrzam, [])
            
            # Sprawdź czy ktoś pracuje nad tym casem
            active = [e for e in existing_list if e["status"] in ("przydzielony", "w_toku")]
            if active:
                skipped += 1
                continue
            
            # Usuń WSZYSTKIE stare wolne/zakończone z tym NrZam
            for e in existing_list:
                if e["status"] in ("wolny", "zakonczony"):
                    db.collection("ew_cases").document(e["doc_id"]).delete()
                    deleted += 1
            
            case_id = f"{batch_id}_{case.get('grupa', 'XX')}_{i+1:04d}"
            # Odroczony = case którego prompt nie wypisał (dodany przez uzupełnianie brakujących)
            case_status = case.get("_forced_status", "wolny")
            db.collection("ew_cases").document(case_id).set({
                "batch_id": batch_id,
                "numer_zamowienia": nrzam,
                "score": case.get("score", 0),
                "priority_icon": case.get("priority_icon", "⚪"),
                "priority_label": case.get("priority_label", ""),
                "grupa": case.get("grupa") or "",
                "index_handlowy": case.get("index_handlowy", ""),
                "pelna_linia_szturchacza": case.get("pelna_linia_szturchacza", ""),
                "naglowek_priorytetowy": case.get("naglowek_priorytetowy", ""),
                "status": case_status,
                "assigned_to": None,
                "assigned_at": None,
                "completed_at": None,
                "result_tag": None,
                "result_pz": None,
                "sort_order": i,
                "created_at": firestore.SERVER_TIMESTAMP,
            })
            saved += 1
        
        # Zapisz batch info
        db.collection("ew_batches").document(batch_id).set({
            "created_at": firestore.SERVER_TIMESTAMP,
            "created_by": "admin",
            "date_label": now.strftime("%Y-%m-%d"),
            "total_cases": len(batch_cases),
            "status": "active",
            "summary": f"Partia {batch_num}/{total_batches}: {saved} zapisanych, {skipped} pominiętych, {deleted} duplikatów usuniętych",
            "prompt_used": st.session_state.get("_ew_prompt_name", "?"),
            "model_used": st.session_state.get("_ew_model", "?"),
        })
        
        st.toast(f"💾 Partia {batch_num}: {saved} casów zapisanych do bazy" + (f", {skipped} pominiętych" if skipped else ""))

    # --- FUNKCJA PRZELICZANIA JEDNEJ PARTII (z rerun po zakończeniu) ---
    def _do_single_batch(batch_idx):
        """Przelicz jedną partię i zapisz postęp. Po powrocie nastąpi rerun."""
        batches = st.session_state.get("_ew_batches_to_process", [])
        model_choice = st.session_state.get("_ew_model", "gemini-2.5-pro")
        prompt_url = st.session_state.get("_ew_prompt_url", "")
        project = st.session_state.get("_ew_project", "")
        
        WIEZOWIEC_PROMPT = get_remote_prompt(prompt_url)
        if not WIEZOWIEC_PROMPT:
            st.error("Nie udało się pobrać promptu!")
            return
        
        if not GCP_PROJECTS:
            st.error("Brak kluczy GCP!")
            return
        
        cur_swinka = load_wsad("swinka")
        cur_uszki = load_wsad("uszki")
        
        tz_pl = pytz.timezone('Europe/Warsaw')
        now = datetime.now(tz_pl)
        total_batches = len(batches)
        
        safety_settings = [
            SafetySetting(category=HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT, threshold=HarmBlockThreshold.BLOCK_NONE),
            SafetySetting(category=HarmCategory.HARM_CATEGORY_HARASSMENT, threshold=HarmBlockThreshold.BLOCK_NONE),
            SafetySetting(category=HarmCategory.HARM_CATEGORY_HATE_SPEECH, threshold=HarmBlockThreshold.BLOCK_NONE),
            SafetySetting(category=HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT, threshold=HarmBlockThreshold.BLOCK_NONE),
        ]
        
        all_cases = st.session_state.get("_ew_all_cases", [])
        all_raw_outputs = st.session_state.get("_ew_all_raw_outputs", [])
        
        batch_chunk = batches[batch_idx]
        batch_num = batch_idx + 1
        batch_szturchacz = '\n\n'.join([block for _, block in batch_chunk])
        
        progress_bar = st.progress(0, text=f"🏢 Partia {batch_num}/{total_batches} ({len(batch_chunk)} zamówień)...")
        
        # --- ROTACJA KLUCZY: zmień projekt per partia ---
        if GCP_PROJECTS:
            rot_project = GCP_PROJECTS[batch_idx % len(GCP_PROJECTS)]
            st.toast(f"🔑 Partia {batch_num}: klucz {batch_idx % len(GCP_PROJECTS) + 1}/{len(GCP_PROJECTS)} ({rot_project[:20]}...)")
            try:
                ci = json.loads(st.secrets["FIREBASE_CREDS"])
                cv = service_account.Credentials.from_service_account_info(ci)
                vertexai.init(project=rot_project, location=st.secrets.get("GCP_LOCATION", "us-central1"), credentials=cv)
            except Exception as e:
                st.error(f"Błąd Vertex AI (klucz {batch_idx % len(GCP_PROJECTS) + 1}): {e}")
                return
        
        user_msg = f"""Data dzisiejsza: {data_obrobki.strftime('%d.%m.%Y')}

Przelicz priorytety dla poniższych zamówień.
{"Partia " + str(batch_num) + " z " + str(total_batches) + "." if total_batches > 1 else ""}

=== WSAD 1: ŚWINKA ===
{cur_swinka}

=== WSAD 2: SZTURCHACZ — ZAMÓWIENIA DO PRZELICZENIA ({len(batch_chunk)} szt.) ===
{batch_szturchacz}

=== WSAD 3: STANY USZKÓW ===
{cur_uszki if cur_uszki else '(brak danych o uszkach)'}
"""
        
        ai_text = None
        FALLBACK_CHAIN = ["gemini-2.5-pro", "gemini-3-pro-preview", "gemini-3.1-pro-preview"]
        models_to_try = [model_choice]
        for fb in FALLBACK_CHAIN:
            if fb != model_choice and fb not in models_to_try:
                models_to_try.append(fb)
        
        for try_model in models_to_try:
            is_fallback = (try_model != model_choice)
            if is_fallback:
                st.toast(f"🔄 Partia {batch_num}: przełączam na {try_model}...")
            
            for attempt in range(3):  # max 3 próby (nie 5 — websocket timeout)
                try:
                    model = GenerativeModel(try_model, system_instruction=WIEZOWIEC_PROMPT)
                    chat = model.start_chat(response_validation=False)
                    resp = chat.send_message(
                        user_msg,
                        generation_config={"temperature": 0.0, "max_output_tokens": 65536},
                        safety_settings=safety_settings,
                    )
                    if resp.candidates:
                        candidate = resp.candidates[0]
                        if candidate.content and candidate.content.parts:
                            ai_text = candidate.content.parts[0].text
                    else:
                        ai_text = resp.text
                    
                    if ai_text:
                        if is_fallback:
                            st.toast(f"⚡ Partia {batch_num}: odpowiedź z {try_model}")
                        break
                except Exception as e:
                    err_str = str(e)
                    if "429" in err_str or "Quota" in err_str or "ResourceExhausted" in err_str or "503" in err_str or "unavailable" in err_str.lower():
                        # Rotacja klucza przy quota/503
                        if GCP_PROJECTS and len(GCP_PROJECTS) > 1:
                            next_key_idx = (batch_idx + attempt + 1) % len(GCP_PROJECTS)
                            rot_project = GCP_PROJECTS[next_key_idx]
                            try:
                                ci = json.loads(st.secrets["FIREBASE_CREDS"])
                                cv = service_account.Credentials.from_service_account_info(ci)
                                vertexai.init(project=rot_project, location=st.secrets.get("GCP_LOCATION", "us-central1"), credentials=cv)
                                st.toast(f"🔑 Partia {batch_num}: rotacja na klucz {next_key_idx+1}/{len(GCP_PROJECTS)}")
                            except Exception:
                                pass
                        wait_time = min(5 * (attempt + 1), 10)  # 5s, 10s, 10s (max 25s total)
                        st.toast(f"⏳ {try_model}, partia {batch_num}, próba {attempt+1}/3, czekam {wait_time}s...")
                        time.sleep(wait_time)
                    elif "Finish reason: 2" in err_str or "response_validation" in err_str:
                        st.toast(f"⚠️ Safety block, partia {batch_num}, próba {attempt+1}/3...")
                        time.sleep(5)
                    else:
                        st.error(f"Błąd AI ({try_model}, partia {batch_num}): {err_str[:300]}")
                        break
            
            if ai_text:
                break
        
        if ai_text:
            all_raw_outputs.append(f"=== PARTIA {batch_num}/{total_batches} ({len(batch_chunk)} zam.) ===\n{ai_text}")
            batch_cases = parse_wiezowiec_output(ai_text)
            all_cases.extend(batch_cases)
            if batch_cases:
                st.toast(f"✅ Partia {batch_num}: {len(batch_cases)} casów")
                _save_cases_to_db(batch_cases, batch_num, total_batches)
            
            # --- UZUPEŁNIJ BRAKUJĄCE ---
            parsed_nrzams = set(c.get("numer_zamowienia", "") for c in batch_cases)
            input_nrzams = set(nrzam for nrzam, _ in batch_chunk)
            missing_nrzams = input_nrzams - parsed_nrzams
            
            if missing_nrzams:
                missing_cases = []
                for nrzam in missing_nrzams:
                    wsad_block = ""
                    for nr, block in batch_chunk:
                        if nr == nrzam:
                            wsad_block = block
                            break
                    
                    # --- P1: INTELIGENTNY STATUS BRAKUJĄCEGO CASE'A ---
                    # Szukaj prawidłowego tagu: C#:...;NEXT=dd.mm
                    # Prawidłowy = zaczyna się od C# i zawiera ;NEXT=data
                    block_lower = wsad_block.lower()
                    has_delivered = "delivered" in block_lower
                    
                    is_future = False
                    has_valid_tag = False
                    tag_match = re.search(r'c#:.*?;next=(\d{2}\.\d{2})', wsad_block, re.IGNORECASE)
                    if tag_match and data_obrobki:
                        has_valid_tag = True
                        try:
                            ns = tag_match.group(1)  # dd.mm
                            nd = datetime.strptime(ns + f".{data_obrobki.year}", "%d.%m.%Y").date()
                            if nd > data_obrobki:
                                is_future = True
                        except:
                            pass
                    
                    if not wsad_block.strip():
                        reason = "pusty_blok"
                        forced_status = "odroczony"
                    elif has_valid_tag and is_future:
                        reason = f"odroczony (tag NEXT={tag_match.group(1)})"
                        forced_status = "odroczony"
                    elif has_valid_tag and not is_future:
                        reason = f"termin_ok (tag NEXT={tag_match.group(1)})"
                        forced_status = "wolny"
                    elif has_delivered:
                        reason = "prompt_pominął (Delivered, brak tagu)"
                        forced_status = "wolny"
                    else:
                        reason = "brak_delivered"
                        forced_status = "odroczony"
                    # --- KONIEC P1 ---
                    
                    missing_cases.append({
                        "numer_zamowienia": nrzam,
                        "score": 0,
                        "priority_icon": "⚪",
                        "priority_label": f"NIEPRZYDZIELONY — {reason}",
                        "grupa": "",
                        "index_handlowy": "",
                        "pelna_linia_szturchacza": wsad_block,
                        "naglowek_priorytetowy": f"[SCORE=0] ⚪ | {reason}",
                        "_forced_status": forced_status,
                    })
                if missing_cases:
                    _save_cases_to_db(missing_cases, batch_num, total_batches)
                    st.toast(f"📋 Partia {batch_num}: {len(missing_cases)} casów nieprzydzielonych dodano do bazy")
            
            if not batch_cases and not missing_nrzams:
                st.toast(f"ℹ️ Partia {batch_num}: 0 casów po filtracji")
        else:
            all_raw_outputs.append(f"=== PARTIA {batch_num}/{total_batches} — BRAK ODPOWIEDZI ===")
            st.warning(f"⚠️ Partia {batch_num}: brak odpowiedzi AI")
        
        progress_bar.progress(1.0, text=f"✅ Partia {batch_num} gotowa!")
        
        # Zapisz postęp NATYCHMIAST (nie czekaj na resztę)
        st.session_state["_ew_batches_done"] = batch_idx + 1
        st.session_state["_ew_all_cases"] = all_cases
        st.session_state["_ew_all_raw_outputs"] = all_raw_outputs
        st.session_state["_ew_raw_ai_output"] = '\n\n'.join(all_raw_outputs)
    
    # --- PANEL PRZELICZANIA PARTII (przyciski) ---
    if batches_to_process:
        total_batches = len(batches_to_process)
        batches_done = st.session_state.get("_ew_batches_done", 0)
        
        # AUTO-CONTINUE: jeśli flaga ustawiona i zostały partie → przelicz następną
        if st.session_state.get("_ew_auto_continue") and batches_done < total_batches:
            st.info(f"🔄 Auto-continue: partia {batches_done+1}/{total_batches}...")
            _do_single_batch(batches_done)
            st.rerun()
        
        st.markdown("---")
        st.markdown(f"### 📦 Partycje: {batches_done}/{total_batches} przeliczonych")
        
        # Pasek postępu globalny
        if batches_done > 0:
            st.progress(batches_done / total_batches, text=f"✅ {batches_done}/{total_batches} partii gotowych")
        
        # Info per partia
        for bi, bc in enumerate(batches_to_process):
            status_icon = "✅" if bi < batches_done else ("⏳" if bi == batches_done else "⬜")
            st.caption(f"{status_icon} Partia {bi+1}: {len(bc)} zamówień")
        
        if batches_done < total_batches:
            if not data_obrobki:
                st.error("⚠️ Wybierz datę obróbki żeby rozpocząć przeliczanie!")
            else:
                if st.session_state.get("_ew_auto_continue"):
                    st.warning(f"🔄 Tryb automatyczny — przelicza partie jedna po drugiej.")
                    if st.button("⏸️ STOP auto-continue"):
                        st.session_state.pop("_ew_auto_continue", None)
                        st.rerun()
                else:
                    col_btn1, col_btn2 = st.columns(2)
                    with col_btn1:
                        if st.button(f"🚀 Przelicz następną paczkę (partia {batches_done+1})", type="primary"):
                            _do_single_batch(batches_done)
                            st.rerun()
                    with col_btn2:
                        if st.button(f"🚀 Przelicz wszystkie pozostałe ({total_batches - batches_done} partii)"):
                            st.session_state["_ew_auto_continue"] = True
                            _do_single_batch(batches_done)
                            st.rerun()
        else:
            st.session_state.pop("_ew_auto_continue", None)
            st.success(f"✅ Wszystkie {total_batches} partii przeliczone!")
            
            # === AUTO-START AUTOPILOTA na X% ===
            if autopilot_pct > 0 and gen_obsada and not st.session_state.get("_ew_autopilot_started"):
                ap_state = get_autopilot_status().get("state", "idle")
                if ap_state == "idle":
                    work_date_str = data_obrobki.strftime('%d.%m') if data_obrobki else "?"
                    case_queue, total_wolne = build_autopilot_queue(autopilot_pct, gen_obsada, work_date_str)
                    
                    if case_queue:
                        set_autopilot_status({
                            "state": "running",
                            "processed": 0,
                            "total": len(case_queue),
                            "current_nrzam": "",
                            "last_error": "",
                            "pause_seconds": ap_pause,
                            "model": ap_model,
                            "prompt_url": ap_prompt_url,
                            "prompt_name": ap_prompt_name,
                            "work_date": work_date_str,
                            "tryb": "od_szturchacza",
                            "key_indices": ap_key_indices,
                            "obsada": {g: ops for g, ops in gen_obsada.items()},
                            "started_at": firestore.SERVER_TIMESTAMP,
                        })
                        db.collection("autopilot_config").document("queue").set({
                            "cases": case_queue,
                        })
                        st.session_state["_ew_autopilot_started"] = True
                        st.toast(f"🤖 Autopilot auto-start: {len(case_queue)} casów ({autopilot_pct}% z {total_wolne})")
                        st.rerun()
                    else:
                        st.info("🤖 Autopilot: 0 casów do przeliczenia (brak obsady lub wolnych).")
                elif ap_state == "running":
                    st.info("🤖 Autopilot działa — przejdź do zakładki **Dolewka + Status**.")
        
        # Reset
        if st.button("🗑️ Wyczyść partycje (zacznij od nowa)"):
            for k in list(st.session_state.keys()):
                if k.startswith("_ew_"):
                    del st.session_state[k]
            st.rerun()
    
    # Podgląd surowego outputu (jeśli jest)
    raw_output = st.session_state.get("_ew_raw_ai_output", "")
    if raw_output:
        with st.expander("📄 Surowy wynik AI (kliknij żeby zobaczyć)", expanded=False):
            st.text(raw_output[:20000])
        
        all_cases = st.session_state.get("_ew_all_cases", [])
        if all_cases:
            de = [c for c in all_cases if c.get("grupa") == "DE"]
            fr = [c for c in all_cases if c.get("grupa") == "FR"]
            ukpl = [c for c in all_cases if c.get("grupa") == "UKPL"]
            st.success(f"📊 Dotychczas przeliczono: **{len(all_cases)}** casów — DE={len(de)} | FR={len(fr)} | UKPL={len(ukpl)}")


# ==========================================
# 🤖 ZAKŁADKA: DOLEWKA + STATUS
# ==========================================
with tab_autopilot:
    st.subheader("🤖 Dolewka + Status autopilota")
    
    ap_status = get_autopilot_status()
    state = ap_status.get("state", "idle")
    
    # --- OBSADA DOLEWKI ---
    st.markdown("### 👥 Obsada dolewki")
    ALL_OPERATORS_LIST_DL = ["Emilia", "Oliwia", "Magda", "Ewelina", "Iwona", "Marlena", "Sylwia", "EwelinaG", "Andrzej", "Marta", "Klaudia", "Kasia", "Romana"]
    ROLE_TO_GRUPA_DL = {"Operatorzy_DE": "DE", "Operatorzy_FR": "FR", "Operatorzy_UK/PL": "UKPL"}
    ops_by_grupa_dl = {"DE": [], "FR": [], "UKPL": []}
    for op_c in ALL_OPERATORS_LIST_DL:
        try:
            cfg_doc = db.collection("operator_configs").document(op_c).get()
            if cfg_doc.exists:
                role = cfg_doc.to_dict().get("role", "Operatorzy_DE")
                grupa = ROLE_TO_GRUPA_DL.get(role, "DE")
                ops_by_grupa_dl[grupa].append(op_c)
            else:
                ops_by_grupa_dl["DE"].append(op_c)
        except Exception:
            ops_by_grupa_dl["DE"].append(op_c)
    
    # Domyślna obsada z taba Generuj (jeśli była ustawiona)
    gen_obs = st.session_state.get("_gen_obsada", {})
    
    col_do1, col_do2, col_do3 = st.columns(3)
    with col_do1:
        dl_ops_de = st.multiselect("🇩🇪 DE:", ops_by_grupa_dl["DE"], default=[o for o in gen_obs.get("DE", []) if o in ops_by_grupa_dl["DE"]], key="dl_ops_de")
    with col_do2:
        dl_ops_fr = st.multiselect("🇫🇷 FR:", ops_by_grupa_dl["FR"], default=[o for o in gen_obs.get("FR", []) if o in ops_by_grupa_dl["FR"]], key="dl_ops_fr")
    with col_do3:
        dl_ops_ukpl = st.multiselect("🇬🇧 UKPL:", ops_by_grupa_dl["UKPL"], default=[o for o in gen_obs.get("UKPL", []) if o in ops_by_grupa_dl["UKPL"]], key="dl_ops_ukpl")
    
    dl_obsada = {}
    if dl_ops_de: dl_obsada["DE"] = dl_ops_de
    if dl_ops_fr: dl_obsada["FR"] = dl_ops_fr
    if dl_ops_ukpl: dl_obsada["UKPL"] = dl_ops_ukpl
    
    # --- PARAMETRY AUTOPILOTA (dolewka) ---
    with st.expander("⚙️ Parametry autopilota (dolewka)", expanded=False):
        PROMPT_URLS_hardcoded_dl = {
            "Prompt Stabilny (prompt4624)": "https://raw.githubusercontent.com/szturchaczysko-cpu/szturchacz/refs/heads/main/prompt4624.txt",
        }
        custom_prompts_dl = (db.collection("admin_config").document("custom_prompts").get().to_dict() or {}).get("urls", {})
        ALL_OP_PROMPT_URLS_DL = {**PROMPT_URLS_hardcoded_dl, **custom_prompts_dl}
        
        col_dlp1, col_dlp2, col_dlp3 = st.columns(3)
        with col_dlp1:
            dl_prompt_name = st.selectbox("Prompt operatorski:", list(ALL_OP_PROMPT_URLS_DL.keys()), key="dl_prompt")
            dl_prompt_url = ALL_OP_PROMPT_URLS_DL[dl_prompt_name]
        with col_dlp2:
            dl_pause = st.slider("⏱️ Pauza (sek):", min_value=5, max_value=120, value=30, step=5, key="dl_pause")
            dl_model = st.selectbox("Model AI:", ["gemini-2.5-pro", "gemini-2.5-flash"], key="dl_model")
        with col_dlp3:
            dl_available_keys = [f"{i+1} - {p}" for i, p in enumerate(GCP_PROJECTS)]
            dl_keys = st.multiselect("🔑 Klucze do rotacji:", dl_available_keys, default=dl_available_keys, key="dl_keys")
            dl_key_indices = [int(k.split(" - ")[0]) - 1 for k in dl_keys]
        
        dl_work_date = st.date_input("📅 Data obróbki:", value=datetime.now(pytz.timezone('Europe/Warsaw')).date(), key="dl_work_date")
    
    # --- BAK per grupa ---
    st.markdown("### 🛢️ Bak — przeliczone casy w rezerwie per grupa")
    st.caption("Ile casów autopilotem przeliczonych jeszcze czeka na operatorów (wolne + calculated)")
    
    bak_docs = db.collection("ew_cases").where("status", "==", "wolny").get()
    bak_data = {"DE": {"w_baku": 0, "do_dolania": 0}, "FR": {"w_baku": 0, "do_dolania": 0}, "UKPL": {"w_baku": 0, "do_dolania": 0}}
    
    for bdoc in bak_docs:
        d = bdoc.to_dict()
        g = d.get("grupa", "")
        if g in bak_data:
            if d.get("autopilot_status") == "calculated":
                bak_data[g]["w_baku"] += 1
            else:
                bak_data[g]["do_dolania"] += 1
    
    col_bak1, col_bak2, col_bak3 = st.columns(3)
    dolewka_pcts = {}
    
    for col_b, gname, flag in [(col_bak1, "DE", "🇩🇪"), (col_bak2, "FR", "🇫🇷"), (col_bak3, "UKPL", "🇬🇧")]:
        with col_b:
            bd = bak_data[gname]
            total_g = bd["w_baku"] + bd["do_dolania"]
            st.markdown(f"**{flag} {gname}**")
            st.metric(f"🛢️ W baku", bd["w_baku"])
            st.caption(f"Do dolania: {bd['do_dolania']} | Razem wolnych: {total_g}")
            dolewka_pcts[gname] = st.slider(f"Dolej %:", min_value=0, max_value=100, value=0, step=5, key=f"dolej_{gname}")
    
    # --- 📊 STAN OPERATORÓW (bieżące przeliczenie) ---
    st.markdown("### 📊 Stan operatorów — bieżące przeliczenie")
    st.caption("Dane z aktualnej puli casów (ew_cases)")
    
    all_cases_stats = db.collection("ew_cases").limit(5000).get()
    op_stats = {}
    grupa_stats = {"DE": {"wolny": 0, "przydzielony": 0, "w_toku": 0, "zakonczony": 0, "total": 0},
                   "FR": {"wolny": 0, "przydzielony": 0, "w_toku": 0, "zakonczony": 0, "total": 0},
                   "UKPL": {"wolny": 0, "przydzielony": 0, "w_toku": 0, "zakonczony": 0, "total": 0}}
    
    for cdoc in all_cases_stats:
        d = cdoc.to_dict()
        s = d.get("status", "wolny")
        g = d.get("grupa", "")
        op = d.get("assigned_to", "")
        
        if g in grupa_stats:
            grupa_stats[g][s] = grupa_stats[g].get(s, 0) + 1
            grupa_stats[g]["total"] += 1
        
        if op and s in ("przydzielony", "w_toku", "zakonczony"):
            if op not in op_stats:
                op_stats[op] = {"pobrane": 0, "zakonczony": 0, "w_toku": 0, "grupa": g}
            op_stats[op]["pobrane"] += 1
            if s == "zakonczony":
                op_stats[op]["zakonczony"] += 1
            elif s in ("przydzielony", "w_toku"):
                op_stats[op]["w_toku"] += 1
    
    # Podsumowanie per grupa
    col_gs1, col_gs2, col_gs3 = st.columns(3)
    for col_gs, gname, flag in [(col_gs1, "DE", "🇩🇪"), (col_gs2, "FR", "🇫🇷"), (col_gs3, "UKPL", "🇬🇧")]:
        with col_gs:
            gs = grupa_stats[gname]
            done = gs.get("zakonczony", 0)
            total = gs["total"]
            pct = round(done / total * 100, 1) if total > 0 else 0
            st.markdown(f"**{flag} {gname}**: {done}/{total} ({pct}%)")
            st.progress(pct / 100)
    
    # Tabela per operator
    if op_stats:
        op_rows = []
        for op, counts in sorted(op_stats.items(), key=lambda x: x[1]["zakonczony"], reverse=True):
            op_rows.append({
                "Operator": op,
                "Grupa": counts.get("grupa", "?"),
                "📥 Pobrane": counts["pobrane"],
                "✅ Zakończone": counts["zakonczony"],
                "🔄 W toku": counts["w_toku"],
            })
        df_op_stats = pd.DataFrame(op_rows)
        st.dataframe(df_op_stats, use_container_width=True, hide_index=True)
    else:
        st.info("Brak danych — operatorzy jeszcze nie brali casów.")
    
    # Dolewka button
    if any(v > 0 for v in dolewka_pcts.values()):
        # Policz ile casów do dolania
        dolewka_summary = []
        for g, pct in dolewka_pcts.items():
            if pct > 0:
                count = max(1, int(bak_data[g]["do_dolania"] * pct / 100))
                dolewka_summary.append(f"{g}: {count} ({pct}% z {bak_data[g]['do_dolania']})")
        st.info(f"🎯 Dolewka: {' | '.join(dolewka_summary)}")
        
        if state == "running":
            st.warning("⚠️ Autopilot jeszcze działa — poczekaj aż skończy.")
        elif st.button("🤖 Przepilotuj dolewkę", type="primary"):
            if not dl_obsada:
                st.error("⚠️ Wybierz operatorów powyżej!")
            else:
                # Zbierz casy do dolania per grupa
                dolewka_queue = []
                group_counters = {g: 0 for g in dl_obsada}
                
                for g, pct in dolewka_pcts.items():
                    if pct <= 0 or g not in dl_obsada or not dl_obsada[g]:
                        continue
                    g_cases = []
                    for bdoc in bak_docs:
                        d = bdoc.to_dict()
                        if d.get("grupa") == g and d.get("autopilot_status") != "calculated":
                            d["_doc_id"] = bdoc.id
                            g_cases.append(d)
                    g_cases.sort(key=lambda c: -c.get("score", 0))
                    count = max(1, int(len(g_cases) * pct / 100))
                    top_g = g_cases[:count]
                    
                    ops = dl_obsada[g]
                    for wc in top_g:
                        assigned_op = ops[group_counters[g] % len(ops)]
                        group_counters[g] += 1
                        dolewka_queue.append({
                            "doc_id": wc["_doc_id"],
                            "nrzam": wc.get("numer_zamowienia", "?"),
                            "operator": assigned_op,
                            "grupa": g,
                            "grupa_operatorska": GRUPA_MAP_GLOBAL.get(g, "Operatorzy_DE"),
                        })
                        db.collection("ew_cases").document(wc["_doc_id"]).update({
                            "autopilot_assigned_to": assigned_op,
                        })
                
                if dolewka_queue:
                    work_date_str = dl_work_date.strftime('%d.%m')
                    
                    set_autopilot_status({
                        "state": "running",
                        "processed": 0,
                        "total": len(dolewka_queue),
                        "current_nrzam": "",
                        "last_error": "",
                        "pause_seconds": dl_pause,
                        "model": dl_model,
                        "prompt_url": dl_prompt_url,
                        "prompt_name": dl_prompt_name,
                        "work_date": work_date_str,
                        "tryb": "od_szturchacza",
                        "key_indices": dl_key_indices,
                        "obsada": {g: ops for g, ops in dl_obsada.items()},
                        "started_at": firestore.SERVER_TIMESTAMP,
                    })
                    db.collection("autopilot_config").document("queue").set({
                        "cases": dolewka_queue,
                    })
                    st.success(f"🤖 Dolewka uruchomiona: {len(dolewka_queue)} casów!")
                    st.rerun()
                else:
                    st.warning("Brak casów do dolania (brak obsady lub 0 nieprzeliczonych).")
    
    # --- STATUS AUTOPILOTA ---
    st.markdown("---")
    st.markdown("### 📊 Status autopilota")
    
    if state == "running":
        processed = ap_status.get("processed", 0)
        total = ap_status.get("total", 0)
        current = ap_status.get("current_nrzam", "")
        pct = processed / max(total, 1)
        
        st.warning(f"🔄 **Autopilot działa** — {processed}/{total} casów przeliczonych")
        st.progress(pct, text=f"Case {processed+1}/{total}: {current}")
        
        if ap_status.get("last_error"):
            st.error(f"Ostatni błąd: {ap_status['last_error']}")
        
        col_stop1, col_stop2 = st.columns(2)
        with col_stop1:
            if st.button("⏸️ STOP Autopilot", type="primary"):
                set_autopilot_status({"state": "stopping"})
                st.rerun()
        with col_stop2:
            if st.button("🔄 Odśwież postęp"):
                st.rerun()
    
    elif state == "stopping":
        processed = ap_status.get("processed", 0)
        total = ap_status.get("total", 0)
        st.warning(f"⏸️ Autopilot zatrzymany po {processed}/{total} casach.")
        col_r1, col_r2 = st.columns(2)
        with col_r1:
            if st.button("▶️ Wznów od miejsca zatrzymania", type="primary"):
                set_autopilot_status({"state": "running"})
                st.rerun()
        with col_r2:
            if st.button("🔄 Reset (zacznij od nowa)"):
                set_autopilot_status({"state": "idle", "processed": 0, "total": 0, "current_nrzam": "", "last_error": ""})
                st.rerun()
    
    elif state == "done":
        processed = ap_status.get("processed", 0)
        total = ap_status.get("total", 0)
        st.success(f"✅ **Autopilot zakończony** — przeliczono {processed}/{total} casów")
        st.progress(1.0)
        if st.button("🔄 Reset (nowa sesja)"):
            set_autopilot_status({"state": "idle", "processed": 0, "total": 0, "current_nrzam": "", "last_error": ""})
            st.rerun()
    
    else:  # idle
        st.info("💤 Autopilot nieaktywny. Uruchom z zakładki 'Generuj + Autopilot' lub użyj dolewki powyżej.")
    
    # --- CZYSZCZENIE ---
    st.markdown("---")
    with st.expander("🧹 Zarządzanie przeliczeniami nocnymi"):
        st.caption("Wyczyść nocne przeliczenia (autopilot_messages) z casów w bazie.")
        col_clean1, col_clean2 = st.columns(2)
        with col_clean1:
            if st.button("🧹 Wyczyść przeliczenia nocne", type="secondary"):
                all_docs = db.collection("ew_cases").limit(5000).get()
                cleared = 0
                for doc in all_docs:
                    d = doc.to_dict()
                    if d.get("autopilot_messages") or d.get("autopilot_status") == "calculated":
                        db.collection("ew_cases").document(doc.id).update({
                            "autopilot_messages": firestore.DELETE_FIELD,
                            "autopilot_status": firestore.DELETE_FIELD,
                            "autopilot_operator": firestore.DELETE_FIELD,
                            "autopilot_date": firestore.DELETE_FIELD,
                            "autopilot_calculated_at": firestore.DELETE_FIELD,
                            "autopilot_model": firestore.DELETE_FIELD,
                            "autopilot_project": firestore.DELETE_FIELD,
                            "autopilot_assigned_to": firestore.DELETE_FIELD,
                        })
                        cleared += 1
                set_autopilot_status({"state": "idle", "processed": 0, "total": 0})
                try:
                    db.collection("autopilot_config").document("queue").delete()
                except:
                    pass
                st.success(f"✅ Wyczyszczono nocne przeliczenia z {cleared} casów.")
                st.rerun()
        with col_clean2:
            try:
                all_docs_check = db.collection("ew_cases").limit(5000).get()
                with_autopilot = sum(1 for d in all_docs_check if d.to_dict().get("autopilot_status") == "calculated")
                st.info(f"🤖 Casów z nocnym przeliczeniem: **{with_autopilot}**")
            except:
                pass

    # ===========================================
    # PĘTLA AUTOPILOTA (działa gdy state=running)
    # Przetwarzaj JEDEN case per rerun żeby websocket nie padł
    # ===========================================
    if state == "running":
        # Pobierz konfigurację
        ap_cfg = get_autopilot_status()
        queue_doc = db.collection("autopilot_config").document("queue").get()
        if not queue_doc.exists:
            set_autopilot_status({"state": "idle", "last_error": "Brak kolejki casów"})
            st.rerun()
        else:
            queue = queue_doc.to_dict().get("cases", [])
            processed = ap_cfg.get("processed", 0)
            total = len(queue)
            pause_sec = ap_cfg.get("pause_seconds", 30)
            model_id = ap_cfg.get("model", "gemini-2.5-pro")
            prompt_url = ap_cfg.get("prompt_url", "")
            work_date = ap_cfg.get("work_date", "")
            tryb = ap_cfg.get("tryb", "od_szturchacza")
            key_indices = ap_cfg.get("key_indices", [0])

            # Fallback daty
            if not work_date:
                tz_pl = pytz.timezone('Europe/Warsaw')
                work_date = datetime.now(tz_pl).strftime('%d.%m')

            # Sprawdź czy jest jeszcze coś do zrobienia
            if processed >= total:
                set_autopilot_status({"state": "done", "processed": total, "current_nrzam": ""})
                st.balloons()
                st.rerun()
            else:
                # Znajdź następny case do przeliczenia (skip pominiętych)
                idx = processed
                case_info = None
                while idx < total:
                    candidate = queue[idx]
                    doc_id = candidate["doc_id"]
                    case_doc = db.collection("ew_cases").document(doc_id).get()
                    if not case_doc.exists:
                        st.caption(f"⚠️ {candidate['nrzam']}: usunięty, pomijam")
                        idx += 1
                        continue
                    case_data = case_doc.to_dict()
                    if case_data.get("status") != "wolny":
                        st.caption(f"⏭️ {candidate['nrzam']}: status={case_data.get('status')} — pomijam")
                        idx += 1
                        continue
                    if case_data.get("autopilot_status") == "calculated":
                        st.caption(f"✅ {candidate['nrzam']}: już przeliczone — pomijam")
                        idx += 1
                        continue
                    wsad = case_data.get("pelna_linia_szturchacza", "")
                    if not wsad:
                        st.caption(f"⚠️ {candidate['nrzam']}: brak wsadu — pomijam")
                        idx += 1
                        continue
                    case_info = candidate
                    break

                if case_info is None:
                    # Wszystkie pominięte/przeliczone
                    set_autopilot_status({"state": "done", "processed": total, "current_nrzam": ""})
                    st.balloons()
                    st.rerun()
                else:
                    # Przelicz JEDEN case
                    doc_id = case_info["doc_id"]
                    nrzam = case_info["nrzam"]
                    case_operator = case_info.get("operator", "Autopilot")
                    case_grupa_op = case_info.get("grupa_operatorska", "Operatorzy_DE")

                    set_autopilot_status({"processed": idx, "current_nrzam": nrzam, "last_error": ""})
                    st.info(f"🤖 Case {idx+1}/{total}: **{nrzam}** — odpytywanie AI...")

                    # Pobierz prompt operatorski
                    OP_PROMPT = get_remote_prompt(prompt_url)
                    if not OP_PROMPT:
                        set_autopilot_status({"state": "done", "last_error": "Nie udało się pobrać promptu operatorskiego"})
                        st.rerun()

                    parametry = f"""
# PARAMETRY STARTOWE
domyslny_operator={case_operator}
domyslna_data={work_date}
Grupa_Operatorska={case_grupa_op}
domyslny_tryb={tryb}
notag=TAK
analizbior=NIE
"""
                    FULL_PROMPT = OP_PROMPT + parametry

                    # Safety settings
                    safety_settings = [
                        SafetySetting(category=HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT, threshold=HarmBlockThreshold.BLOCK_NONE),
                        SafetySetting(category=HarmCategory.HARM_CATEGORY_HARASSMENT, threshold=HarmBlockThreshold.BLOCK_NONE),
                        SafetySetting(category=HarmCategory.HARM_CATEGORY_HATE_SPEECH, threshold=HarmBlockThreshold.BLOCK_NONE),
                        SafetySetting(category=HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT, threshold=HarmBlockThreshold.BLOCK_NONE),
                    ]

                    # --- ROTACJA KLUCZY ---
                    key_idx = key_indices[idx % len(key_indices)]
                    project = GCP_PROJECTS[key_idx]

                    try:
                        ci = json.loads(st.secrets["FIREBASE_CREDS"])
                        cv = service_account.Credentials.from_service_account_info(ci)
                        vertexai.init(project=project, location=st.secrets.get("GCP_LOCATION", "us-central1"), credentials=cv)
                    except Exception as e:
                        set_autopilot_status({"last_error": f"Vertex init error: {str(e)[:200]}"})
                        st.error(f"❌ {nrzam}: Vertex init error — {str(e)[:200]}")
                        # Przejdź do następnego na rerun
                        set_autopilot_status({"processed": idx + 1})
                        time.sleep(3)
                        st.rerun()

                    # --- WYWOŁANIE AI (kaskadowy fallback) ---
                    ai_response = None
                    FALLBACK_CHAIN_AP = ["gemini-2.5-pro", "gemini-3-pro-preview", "gemini-3.1-pro-preview"]
                    ap_models_to_try = [model_id]
                    for fb in FALLBACK_CHAIN_AP:
                        if fb != model_id and fb not in ap_models_to_try:
                            ap_models_to_try.append(fb)

                    used_ap_model = model_id
                    for try_model in ap_models_to_try:
                        for attempt in range(3):
                            try:
                                model = GenerativeModel(try_model, system_instruction=FULL_PROMPT)
                                chat = model.start_chat(response_validation=False)
                                resp = chat.send_message(
                                    wsad,
                                    generation_config={"temperature": 0.0, "max_output_tokens": 8192},
                                    safety_settings=safety_settings,
                                )
                                if resp.candidates and resp.candidates[0].content and resp.candidates[0].content.parts:
                                    ai_response = resp.candidates[0].content.parts[0].text
                                else:
                                    ai_response = resp.text
                                used_ap_model = try_model
                                break
                            except Exception as e:
                                err_str = str(e)
                                if "429" in err_str or "Quota" in err_str or "ResourceExhausted" in err_str or "503" in err_str or "unavailable" in err_str.lower():
                                    wait_time = min(5 * (attempt + 1), 10)  # 5s, 10s, 10s
                                    st.caption(f"⏳ {try_model}, {nrzam}, próba {attempt+1}/3, czekam {wait_time}s...")
                                    time.sleep(wait_time)
                                else:
                                    set_autopilot_status({"last_error": f"{nrzam}: {err_str[:200]}"})
                                    st.caption(f"⚠️ {nrzam}: {try_model} — {err_str[:100]}")
                                    break

                        if ai_response:
                            break

                    # --- ZAPIS WYNIKU ---
                    if ai_response:
                        # --- E3: FORUM INTEGRATION (autopilot) ---
                        # Pętla: AI → markery → wykonaj → jeśli READ → re-send z kontekstem → powtórz
                        autopilot_conversation = [
                            {"role": "user", "content": wsad},
                            {"role": "model", "content": ai_response},
                        ]
                        
                        if FORUM_ENABLED:
                            for forum_iter in range(3):  # max 3 iteracje forum
                                if "[FORUM_WRITE|" not in ai_response and "[FORUM_READ|" not in ai_response:
                                    break
                                
                                forum_result = execute_forum_actions(ai_response)
                                ai_response = forum_result["response"]
                                autopilot_conversation[-1]["content"] = ai_response
                                
                                # FORUM_WRITE → loguj wyniki
                                for fw in forum_result.get("forum_writes", []):
                                    if fw.get("success"):
                                        st.caption(f"  📤 Forum WRITE: post {fw.get('FORUM_ID', '?')} wysłany")
                                    else:
                                        st.caption(f"  ❌ Forum WRITE: {fw.get('error', '?')}")
                                
                                # FORUM_READ → wstrzyknij kontekst i odpytaj AI ponownie
                                if forum_result.get("forum_reads"):
                                    forum_context = "\n\n".join(forum_result["forum_reads"])
                                    st.caption(f"  📖 Forum READ: wstrzykuję kontekst ({len(forum_context)} zn.)")
                                    
                                    # Dodaj kontekst do konwersacji
                                    autopilot_conversation.append({"role": "user", "content": forum_context})
                                    
                                    # Re-send do AI z pełną historią
                                    try:
                                        history_for_resend = []
                                        for msg in autopilot_conversation:
                                            role_vertex = "user" if msg["role"] == "user" else "model"
                                            history_for_resend.append(
                                                Content(role=role_vertex, parts=[Part.from_text(msg["content"])])
                                            )
                                        
                                        # Wyślij ostatni message (forum_context)
                                        model_resend = GenerativeModel(used_ap_model, system_instruction=FULL_PROMPT)
                                        chat_resend = model_resend.start_chat(
                                            history=history_for_resend[:-1],
                                            response_validation=False
                                        )
                                        resp_resend = chat_resend.send_message(
                                            forum_context,
                                            generation_config={"temperature": 0.0, "max_output_tokens": 8192},
                                            safety_settings=safety_settings,
                                        )
                                        if resp_resend.candidates and resp_resend.candidates[0].content and resp_resend.candidates[0].content.parts:
                                            ai_response = resp_resend.candidates[0].content.parts[0].text
                                        else:
                                            ai_response = resp_resend.text
                                        
                                        autopilot_conversation.append({"role": "model", "content": ai_response})
                                        st.caption(f"  🤖 AI re-response po forum ({len(ai_response)} zn.)")
                                    except Exception as e_forum:
                                        st.caption(f"  ⚠️ Forum re-send error: {str(e_forum)[:100]}")
                                        break
                                else:
                                    break  # Tylko WRITE, bez READ → nie trzeba ponownie pytać AI
                        # --- KONIEC E3 ---
                        
                        db.collection("ew_cases").document(doc_id).update({
                            "autopilot_status": "calculated",
                            "autopilot_messages": autopilot_conversation,
                            "autopilot_calculated_at": firestore.SERVER_TIMESTAMP,
                            "autopilot_model": used_ap_model,
                            "autopilot_project": project,
                            "autopilot_operator": case_operator,
                            "autopilot_date": work_date,
                        })
                        st.success(f"✅ {nrzam}: przeliczone ({len(ai_response)} znaków) — {case_operator} — klucz {key_idx+1}")
                    else:
                        st.warning(f"⚠️ {nrzam}: brak odpowiedzi AI — pomijam")

                    # Zapisz postęp i RERUN (websocket stays alive)
                    set_autopilot_status({"processed": idx + 1, "current_nrzam": ""})

                    # Pauza przed rerun (krótsza niż oryginalna — rerun sam dodaje delay)
                    if idx + 1 < total:
                        time.sleep(min(pause_sec, 10))

                    st.rerun()


# ==========================================
# 📦 HISTORIA PARTII
# ==========================================
with tab_batches:
    st.subheader("📦 Historia partii Wieżowca")
    batches = db.collection("ew_batches").order_by("created_at", direction=firestore.Query.DESCENDING).limit(20).get()
    if not batches:
        st.info("Brak wygenerowanych partii.")
    else:
        for bdoc in batches:
            b = bdoc.to_dict()
            bid = bdoc.id
            ico = "🟢" if b.get("status") == "active" else "⚪"
            with st.expander(f"{ico} {bid} — {b.get('date_label', '?')} | {b.get('summary', '')}"):
                c1, c2 = st.columns(2)
                with c1:
                    st.metric("Casów", b.get("total_cases", 0))
                    st.caption(f"Prompt: {b.get('prompt_used', '?')} | Model: {b.get('model_used', '?')}")
                with c2:
                    batch_cases = db.collection("ew_cases").where("batch_id", "==", bid).get()
                    sc = {"wolny": 0, "przydzielony": 0, "w_toku": 0, "zakonczony": 0}
                    for c in batch_cases:
                        s = c.to_dict().get("status", "wolny")
                        sc[s] = sc.get(s, 0) + 1
                    for k, v in sc.items():
                        st.caption(f"{k}: {v}")
                if b.get("status") == "active":
                    if st.button(f"📥 Archiwizuj", key=f"arch_{bid}"):
                        db.collection("ew_batches").document(bid).update({"status": "archived"})
                        st.rerun()
                
                # Surowy output AI
                raw = b.get("raw_ai_output", "")
                if raw:
                    with st.expander("📄 Surowy wynik AI tego batcha"):
                        st.text(raw[:10000])


# ==========================================
# 📋 PRZEGLĄD CASÓW
# ==========================================
with tab_cases:
    st.subheader("📋 Przegląd casów")
    
    # Pobierz WSZYSTKIE casy raz (dla filtrów i statystyk)
    all_cases_raw = db.collection("ew_cases").order_by("score", direction=firestore.Query.DESCENDING).limit(2000).get()
    all_cases_data = [(d.id, d.to_dict()) for d in all_cases_raw]
    
    # Zbierz unikalne wartości do selectboxów
    all_operators = sorted(set(d.get("assigned_to", "") for _, d in all_cases_data if d.get("assigned_to")))
    all_operators_nocne = sorted(set(d.get("autopilot_assigned_to", "") for _, d in all_cases_data if d.get("autopilot_assigned_to")))
    
    fc1, fc2, fc3, fc4, fc5 = st.columns(5)
    with fc1:
        fg = st.selectbox("Grupa:", ["Wszystkie", "DE", "FR", "UKPL", "Brak grupy / Score 0"])
    with fc2:
        fs = st.selectbox("Status:", ["Wszystkie", "wolny", "przydzielony", "w_toku", "zakonczony", "odroczony", "pominiety"])
    with fc3:
        fo = st.selectbox("Operator:", ["Wszystkie"] + all_operators)
    with fc4:
        fp = st.selectbox("Przeliczenie:", ["Wszystkie", "Przeliczone", "Nieprzeliczone"])
    with fc5:
        f_skip = st.selectbox("Pominięcia:", ["Wszystkie", "Z komentarzem", "Naprawione"], key="f_skip")
    
    # Wyszukiwarka po indexie
    f_index = st.text_input("🔍 Szukaj po indexie:", key="f_index", placeholder="np. 125C514GRUP1")
    
    # Filtrowanie po stronie klienta
    filtered = all_cases_data
    if fg == "Brak grupy / Score 0":
        filtered = [(did, d) for did, d in filtered if not d.get("grupa") or d.get("score", 0) == 0]
    elif fg != "Wszystkie":
        filtered = [(did, d) for did, d in filtered if d.get("grupa") == fg]
    if fs != "Wszystkie":
        filtered = [(did, d) for did, d in filtered if d.get("status") == fs]
    if fo != "Wszystkie":
        filtered = [(did, d) for did, d in filtered if d.get("assigned_to") == fo]
    if fp == "Przeliczone":
        filtered = [(did, d) for did, d in filtered if d.get("autopilot_status") == "calculated"]
    elif fp == "Nieprzeliczone":
        filtered = [(did, d) for did, d in filtered if d.get("autopilot_status") != "calculated"]
    if f_skip == "Z komentarzem":
        filtered = [(did, d) for did, d in filtered if d.get("skip_reason") and not d.get("skip_fixed")]
    elif f_skip == "Naprawione":
        filtered = [(did, d) for did, d in filtered if d.get("skip_fixed")]
    if f_index and f_index.strip():
        idx_q = f_index.strip().lower()
        filtered = [(did, d) for did, d in filtered if idx_q in d.get("index_handlowy", "").lower() or idx_q in d.get("pelna_linia_szturchacza", "").lower()]
    
    if not filtered:
        st.info("Brak casów.")
    else:
        total = len(filtered)
        
        # Statystyki
        n_wolny = sum(1 for _, d in filtered if d.get("status") == "wolny")
        n_przydz = sum(1 for _, d in filtered if d.get("status") in ("przydzielony", "w_toku"))
        n_zakonczony = sum(1 for _, d in filtered if d.get("status") == "zakonczony")
        n_przeliczone = sum(1 for _, d in filtered if d.get("autopilot_status") == "calculated")
        n_nieprzeliczone = total - n_przeliczone
        n_odroczony = sum(1 for _, d in filtered if d.get("status") == "odroczony")
        n_pominiety = sum(1 for _, d in filtered if d.get("status") == "pominiety")
        n_score0 = sum(1 for _, d in all_cases_data if d.get("score", 0) == 0 or not d.get("grupa"))
        st.markdown(f"📊 **Łącznie: {total}** | 🔵 Wolne: {n_wolny} | 🟡 Pobrane: {n_przydz} | 🟢 Zakończone: {n_zakonczony} | ⏭️ Pominięte: {n_pominiety} | ⏸️ Odroczone: {n_odroczony} | 🤖 Przeliczone: {n_przeliczone} | ⚪ Nieprzeliczone: {n_nieprzeliczone}")
        
        # Przycisk: usuń UNKNOWN (śmieci z parsera)
        unknown_cases = [(did, d) for did, d in all_cases_data if d.get("numer_zamowienia", "").startswith("UNKNOWN")]
        if unknown_cases:
            col_unk1, col_unk2 = st.columns([4, 1])
            with col_unk1:
                st.warning(f"⚠️ Znaleziono **{len(unknown_cases)}** casów UNKNOWN (śmieci z parsera — alerty/self-correction)")
            with col_unk2:
                if st.button(f"🗑️ Usuń {len(unknown_cases)} UNKNOWN", key="del_unknown"):
                    for did, _ in unknown_cases:
                        db.collection("ew_cases").document(did).delete()
                    st.success(f"✅ Usunięto {len(unknown_cases)} UNKNOWN z bazy!")
                    st.rerun()
        
        # Przycisk: uwolnij odroczone do kolejki
        odroczone_cases = [(did, d) for did, d in all_cases_data if d.get("status") == "odroczony"]
        if odroczone_cases:
            no_grupa = sum(1 for _, d in odroczone_cases if not d.get("grupa"))
            col_odr1, col_odr2 = st.columns([3, 2])
            with col_odr1:
                st.info(f"⏸️ **{len(odroczone_cases)}** odroczonych casów" +
                        (f" (⚠️ {no_grupa} bez grupy — nie trafią do nikogo!)" if no_grupa else ""))
            with col_odr2:
                force_grupa = st.selectbox("Wymuś grupę (dla brakujących):", ["—", "DE", "FR", "UKPL"], key="odr_force_grupa")
                if st.button(f"☢️ Uwolnij WSZYSTKIE {len(odroczone_cases)} odroczone", key="release_all_odroczone"):
                    for did, d in odroczone_cases:
                        upd = {"status": "wolny"}
                        if force_grupa != "—" and not d.get("grupa"):
                            upd["grupa"] = force_grupa
                        db.collection("ew_cases").document(did).update(upd)
                    st.success(f"✅ Uwolniono {len(odroczone_cases)} casów do kolejki!")
                    st.rerun()
        
        # Paginacja z opcją pokaż wszystkie
        show_all = st.checkbox("📄 Pokaż wszystkie na jednej stronie", key="show_all_cases")
        if show_all:
            PAGE_SIZE = total
            start = 0
            end = total
            st.caption(f"Wszystkie {total} casów")
        else:
            PAGE_SIZE = 50
            total_pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)
            page = st.number_input("Strona:", min_value=1, max_value=total_pages, value=1, step=1)
            start = (page - 1) * PAGE_SIZE
            end = min(start + PAGE_SIZE, total)
            st.caption(f"Strona {page}/{total_pages} (pozycje {start+1}–{end} z {total})")
        
        for doc_id, c in filtered[start:end]:
            smap = {"wolny": "🔵", "przydzielony": "🟡", "w_toku": "🟠", "zakonczony": "🟢", "odroczony": "⏸️", "pominiety": "⏭️"}
            si = smap.get(c.get("status"), "❓")
            ap_mark = "🤖" if c.get("autopilot_status") == "calculated" else ""
            idx_label = f" | 📦 {c.get('index_handlowy')}" if c.get('index_handlowy') else ""
            cc1, cc2 = st.columns([4, 1])
            with cc1:
                st.markdown(f"{si} {ap_mark} **{c.get('numer_zamowienia', '?')}** — "
                            f"{c.get('priority_icon', '')} [{c.get('score', 0)}] {c.get('priority_label', '')}{idx_label}")
            with cc2:
                st.caption(f"{c.get('grupa', '?')} | {c.get('assigned_to') or '-'} | {c.get('status', '?')}")
            
            # Komentarz pominięcia
            if c.get("skip_reason"):
                if c.get("skip_fixed"):
                    st.success(f"✅ Naprawione | ⏭️ Pominięty przez **{c.get('skipped_by', '?')}**: {c.get('skip_reason')}")
                else:
                    sc1, sc2 = st.columns([5, 1])
                    with sc1:
                        st.warning(f"⏭️ Pominięty przez **{c.get('skipped_by', '?')}**: {c.get('skip_reason')}")
                    with sc2:
                        if st.button("✅ Naprawione", key=f"fix_{doc_id}"):
                            upd = {
                                "skip_fixed": True,
                                "skip_fixed_at": firestore.SERVER_TIMESTAMP,
                            }
                            # Jeśli case był pominiety — przywróć do wolnych
                            if c.get("status") == "pominiety":
                                upd["status"] = "wolny"
                            db.collection("ew_cases").document(doc_id).update(upd)
                            st.rerun()
            
            # Przycisk uwolnienia odroczonego z wyborem grupy
            if c.get("status") == "odroczony":
                oc1, oc2, oc3 = st.columns([4, 1, 1])
                with oc1:
                    cur_grupa = c.get("grupa") or "—"
                    st.caption(f"⏸️ Odroczony | grupa: **{cur_grupa}**")
                with oc2:
                    new_grupa = st.selectbox("Grupa:", ["—", "DE", "FR", "UKPL"],
                                            index=["—", "DE", "FR", "UKPL"].index(cur_grupa) if cur_grupa in ["DE", "FR", "UKPL"] else 0,
                                            key=f"grupa_{doc_id}", label_visibility="collapsed")
                with oc3:
                    if st.button("🔓 Uwolnij", key=f"release_{doc_id}"):
                        upd = {"status": "wolny"}
                        if new_grupa != "—":
                            upd["grupa"] = new_grupa
                        db.collection("ew_cases").document(doc_id).update(upd)
                        st.rerun()
            
            # Podgląd nocnego przeliczenia
            if c.get("autopilot_status") == "calculated" and c.get("autopilot_messages"):
                ap_msgs = c["autopilot_messages"]
                ap_op = c.get("autopilot_operator", "?")
                ap_date = c.get("autopilot_date", "?")
                ap_model = c.get("autopilot_model", "?")
                with st.expander(f"🤖 Podgląd nocnego przeliczenia — operator: {ap_op}, data: {ap_date}, model: {ap_model}"):
                    if len(ap_msgs) >= 1:
                        st.markdown("**📥 WSAD (do AI):**")
                        st.code(ap_msgs[0].get("content", "")[:3000], language=None)
                    if len(ap_msgs) >= 2:
                        st.markdown("**🤖 ODPOWIEDŹ AI:**")
                        st.markdown(ap_msgs[1].get("content", "")[:5000])
            
            # Wsad ze szturchacza dla NIEPRZELICZONYCH casów
            elif c.get("pelna_linia_szturchacza"):
                with st.expander(f"📋 Wsad ze szturchacza"):
                    st.code(c["pelna_linia_szturchacza"][:3000], language=None)


# ==========================================
# ⏭️ ZAKŁADKA: POMINIĘTE (ARCHIWUM)
# ==========================================
with tab_skipped:
    st.subheader("⏭️ Pominięte — archiwum nienaprawionych")
    st.caption("Casy przeniesione tutaj po wyczyszczeniu kolejki. Miały komentarz pominięcia bez oznaczenia 'Naprawione'.")
    
    # Pobierz archiwum
    archived_raw = db.collection("ew_cases_archived").order_by("score", direction=firestore.Query.DESCENDING).limit(500).get()
    archived_data = [(d.id, d.to_dict()) for d in archived_raw]
    
    if not archived_data:
        st.info("Brak zarchiwizowanych pominiętych casów.")
    else:
        st.markdown(f"📊 **Łącznie w archiwum: {len(archived_data)}**")
        
        # Filtr po grupie
        arc_grupy = sorted(set(d.get("grupa", "?") for _, d in archived_data))
        arc_fg = st.selectbox("Grupa:", ["Wszystkie"] + arc_grupy, key="arc_fg")
        arc_filtered = archived_data
        if arc_fg != "Wszystkie":
            arc_filtered = [(did, d) for did, d in arc_filtered if d.get("grupa") == arc_fg]
        
        for doc_id, c in arc_filtered:
            idx_label = f" | 📦 {c.get('index_handlowy')}" if c.get('index_handlowy') else ""
            st.markdown(f"⏭️ **{c.get('numer_zamowienia', '?')}** — "
                        f"{c.get('priority_icon', '')} [{c.get('score', 0)}] {c.get('priority_label', '')}{idx_label}")
            
            sc1, sc2, sc3 = st.columns([4, 1, 1])
            with sc1:
                st.warning(f"Pominięty przez **{c.get('skipped_by', '?')}**: {c.get('skip_reason', '')}")
            with sc2:
                if st.button("✅ Naprawione", key=f"arcfix_{doc_id}"):
                    db.collection("ew_cases_archived").document(doc_id).update({
                        "skip_fixed": True,
                        "skip_fixed_at": firestore.SERVER_TIMESTAMP,
                    })
                    st.rerun()
            with sc3:
                if st.button("🗑️ Usuń", key=f"arcdel_{doc_id}"):
                    db.collection("ew_cases_archived").document(doc_id).delete()
                    st.rerun()
            
            # Wsad ze szturchacza
            if c.get("pelna_linia_szturchacza"):
                with st.expander(f"📋 Wsad ze szturchacza"):
                    st.code(c["pelna_linia_szturchacza"][:3000], language=None)
        
        # Przycisk wyczyść całe archiwum
        st.markdown("---")
        if st.button("🗑️ Wyczyść całe archiwum pominiętych", key="clear_archive"):
            for doc_id, _ in archived_data:
                db.collection("ew_cases_archived").document(doc_id).delete()
            st.success(f"🗑️ Usunięto {len(archived_data)} casów z archiwum.")
            st.rerun()
