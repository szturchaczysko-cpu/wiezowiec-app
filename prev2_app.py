import streamlit as st
import vertexai
from vertexai.generative_models import GenerativeModel, Content, Part
from google.oauth2 import service_account
from datetime import datetime
import json, re, pytz, time
import firebase_admin
from firebase_admin import credentials, firestore
import requests

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
    "Wieżowiec v5 (stabilny)": "https://github.com/szturchaczysko-cpu/szturchacz/blob/main/prompt_wiezowiec_v4_gemini.md",
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

def merge_szturchacz(existing_text, new_text):
    """
    Dopełnij istniejący wsad szturchacza nowymi zamówieniami.
    Jeśli zamówienie o tym samym NrZam istnieje — nadpisz nowszą wersją.
    Jeśli nie istnieje — dodaj.
    
    Każde zamówienie to blok tekstu zaczynający się od NrZam.
    """
    def parse_blocks(text):
        """Dzieli tekst na bloki per zamówienie"""
        if not text.strip():
            return {}
        
        blocks = {}
        lines = text.split('\n')
        current_block = []
        current_nr = None
        
        for line in lines:
            # Szukaj NrZam na początku linii lub jako pole
            nr_match = re.search(r'NrZam[:\s]+(\S+)', line, re.IGNORECASE)
            if not nr_match:
                nr_match = re.match(r'^(ZN\d+)', line)
            
            if nr_match:
                # Zapisz poprzedni blok
                if current_nr and current_block:
                    blocks[current_nr] = '\n'.join(current_block)
                # Rozpocznij nowy blok
                current_nr = nr_match.group(1).strip().rstrip(',').rstrip('|')
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
    
    existing_blocks = parse_blocks(existing_text)
    new_blocks = parse_blocks(new_text)
    
    # Merge: nowe nadpisują istniejące, reszta pozostaje
    merged = {**existing_blocks, **new_blocks}
    
    added = len([k for k in new_blocks if k not in existing_blocks])
    updated = len([k for k in new_blocks if k in existing_blocks])
    
    # Złóż z powrotem w tekst
    merged_text = '\n\n'.join(merged.values())
    
    return merged_text, added, updated, len(merged)

def count_lines(text):
    """Policz ile zamówień (bloków) jest w tekście"""
    if not text.strip():
        return 0
    # Policz wystąpienia NrZam
    count = len(re.findall(r'NrZam[:\s]+\S+', text, re.IGNORECASE))
    if count == 0:
        count = len(re.findall(r'ZN\d+', text))
    return max(count, 1 if text.strip() else 0)


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
            
            idx_m = re.search(r'Index:\s*(\S+)', label)
            index_handlowy = idx_m.group(1) if idx_m else ""
            if not index_handlowy:
                lindx_m = re.search(r'lindexy[:\s]+(\S+)', pelna_linia, re.IGNORECASE)
                if lindx_m:
                    index_handlowy = lindx_m.group(1)
            
            if pelna_linia:
                cases.append({
                    "numer_zamowienia": numer or f"UNKNOWN_{len(cases)+1}",
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

tab_wsady, tab_generuj, tab_batches, tab_cases = st.tabs([
    "📂 Wsady",
    "⚡ Generuj raport",
    "📦 Historia partii",
    "📋 Przegląd casów"
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
            # Usuń wszystkie wolne casy z aktywnych batchy
            active_batches = db.collection("ew_batches").where("status", "==", "active").get()
            deleted = 0
            for bdoc in active_batches:
                cases = db.collection("ew_cases").where("batch_id", "==", bdoc.id).where("status", "==", "wolny").get()
                for c in cases:
                    db.collection("ew_cases").document(c.id).delete()
                    deleted += 1
                # Archiwizuj batch
                db.collection("ew_batches").document(bdoc.id).update({"status": "archived"})
            st.success(f"🗑️ Usunięto {deleted} wolnych casów i zarchiwizowano batche.")
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
    
    st.markdown("---")
    
    if st.button("🚀 Generuj raport priorytetów", type="primary"):
        if not current_project:
            st.error("Brak projektu GCP!")
            st.stop()
        
        WIEZOWIEC_PROMPT = get_remote_prompt(sel_prompt_url)
        if not WIEZOWIEC_PROMPT:
            st.error("Nie udało się pobrać promptu!")
            st.stop()
        
        try:
            ci = json.loads(st.secrets["FIREBASE_CREDS"])
            cv = service_account.Credentials.from_service_account_info(ci)
            vertexai.init(project=current_project, location=st.secrets.get("GCP_LOCATION", "us-central1"), credentials=cv)
        except Exception as e:
            st.error(f"Błąd Vertex AI: {e}")
            st.stop()
        
        tz_pl = pytz.timezone('Europe/Warsaw')
        now = datetime.now(tz_pl)
        
        user_msg = f"""Data dzisiejsza: {now.strftime('%d.%m.%Y')}

Generuj raport priorytetów na podstawie poniższych wsadów.

=== WSAD 1: ŚWINKA ===
{cur_swinka}

=== WSAD 2: SZTURCHACZ ===
{cur_szturchacz}

=== WSAD 3: STANY USZKÓW ===
{cur_uszki if cur_uszki else '(brak danych o uszkach)'}
"""
        
        with st.spinner("🏢 Wieżowiec analizuje... To może potrwać kilka minut."):
            ai_text = None
            for attempt in range(3):
                try:
                    model = GenerativeModel(model_choice, system_instruction=WIEZOWIEC_PROMPT)
                    chat = model.start_chat()
                    resp = chat.send_message(user_msg, generation_config={"temperature": 0.0, "max_output_tokens": 65536})
                    ai_text = resp.text
                    break
                except Exception as e:
                    if "429" in str(e) or "Quota" in str(e):
                        st.toast(f"⏳ Limit API, próba {attempt+1}/3...")
                        time.sleep(10)
                    else:
                        st.error(f"Błąd AI: {e}")
                        break
        
        if not ai_text:
            st.error("❌ Brak odpowiedzi AI.")
            st.stop()
        
        with st.expander("📄 Surowy wynik AI", expanded=False):
            st.text(ai_text)
        
        cases = parse_wiezowiec_output(ai_text)
        if not cases:
            st.warning("⚠️ Parser nie znalazł casów. Sprawdź surowy wynik.")
            st.stop()
        
        de = [c for c in cases if c["grupa"] == "DE"]
        fr = [c for c in cases if c["grupa"] == "FR"]
        ukpl = [c for c in cases if c["grupa"] == "UKPL"]
        st.success(f"✅ **{len(cases)}** casów: DE={len(de)} | FR={len(fr)} | UKPL={len(ukpl)}")
        
        pc1, pc2, pc3 = st.columns(3)
        for col, flag, grp in [(pc1, "🇩🇪 DE", de), (pc2, "🇫🇷 FR", fr), (pc3, "🇬🇧 UKPL", ukpl)]:
            with col:
                st.markdown(f"**{flag} ({len(grp)})**")
                for c in grp[:5]:
                    st.caption(f"{c['priority_icon']} [{c['score']}] {c['numer_zamowienia']}")
                if len(grp) > 5:
                    st.caption(f"...+{len(grp)-5} więcej")
        
        st.session_state["_ew_parsed_cases"] = cases
        st.session_state["_ew_prompt_name"] = sel_prompt
        st.session_state["_ew_model"] = model_choice
    
    # Przycisk zapisu
    if st.session_state.get("_ew_parsed_cases"):
        st.markdown("---")
        cases = st.session_state["_ew_parsed_cases"]
        de = [c for c in cases if c["grupa"] == "DE"]
        fr = [c for c in cases if c["grupa"] == "FR"]
        ukpl = [c for c in cases if c["grupa"] == "UKPL"]
        
        if st.button("💾 Zapisz do bazy i udostępnij operatorom", type="primary"):
            tz_pl = pytz.timezone('Europe/Warsaw')
            now = datetime.now(tz_pl)
            batch_id = f"batch_{now.strftime('%Y%m%d_%H%M%S')}"
            
            db.collection("ew_batches").document(batch_id).set({
                "created_at": firestore.SERVER_TIMESTAMP,
                "created_by": "admin",
                "date_label": now.strftime("%Y-%m-%d"),
                "total_cases": len(cases),
                "status": "active",
                "summary": f"DE: {len(de)} | FR: {len(fr)} | UKPL: {len(ukpl)}",
                "prompt_used": st.session_state.get("_ew_prompt_name", "?"),
                "model_used": st.session_state.get("_ew_model", "?"),
            })
            
            progress = st.progress(0)
            for i, case in enumerate(cases):
                case_id = f"{batch_id}_{case['grupa']}_{i+1:04d}"
                db.collection("ew_cases").document(case_id).set({
                    "batch_id": batch_id,
                    "numer_zamowienia": case["numer_zamowienia"],
                    "score": case["score"],
                    "priority_icon": case["priority_icon"],
                    "priority_label": case["priority_label"],
                    "grupa": case["grupa"],
                    "index_handlowy": case.get("index_handlowy", ""),
                    "pelna_linia_szturchacza": case["pelna_linia_szturchacza"],
                    "naglowek_priorytetowy": case["naglowek_priorytetowy"],
                    "status": "wolny",
                    "assigned_to": None,
                    "assigned_at": None,
                    "completed_at": None,
                    "result_tag": None,
                    "result_pz": None,
                    "sort_order": i,
                    "created_at": firestore.SERVER_TIMESTAMP,
                })
                progress.progress((i + 1) / len(cases))
            progress.empty()
            
            st.success(f"✅ Zapisano **{len(cases)}** casów w partii `{batch_id}`!")
            st.session_state["_ew_parsed_cases"] = None
            st.balloons()


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


# ==========================================
# 📋 PRZEGLĄD CASÓW
# ==========================================
with tab_cases:
    st.subheader("📋 Przegląd casów")
    fc1, fc2, fc3 = st.columns(3)
    with fc1:
        fg = st.selectbox("Grupa:", ["Wszystkie", "DE", "FR", "UKPL"])
    with fc2:
        fs = st.selectbox("Status:", ["Wszystkie", "wolny", "przydzielony", "w_toku", "zakonczony"])
    with fc3:
        fo = st.text_input("Operator:", placeholder="np. Emilia")
    
    q = db.collection("ew_cases")
    if fg != "Wszystkie":
        q = q.where("grupa", "==", fg)
    if fs != "Wszystkie":
        q = q.where("status", "==", fs)
    if fo:
        q = q.where("assigned_to", "==", fo)
    q = q.order_by("score", direction=firestore.Query.DESCENDING).limit(100)
    results = q.get()
    
    if not results:
        st.info("Brak casów.")
    else:
        st.caption(f"Pokazuję {len(results)} casów (max 100)")
        for doc in results:
            c = doc.to_dict()
            smap = {"wolny": "🔵", "przydzielony": "🟡", "w_toku": "🟠", "zakonczony": "🟢"}
            si = smap.get(c.get("status"), "❓")
            cc1, cc2 = st.columns([4, 1])
            with cc1:
                st.markdown(f"{si} **{c.get('numer_zamowienia', '?')}** — "
                            f"{c.get('priority_icon', '')} [{c.get('score', 0)}] {c.get('priority_label', '')}")
            with cc2:
                st.caption(f"{c.get('grupa', '?')} | {c.get('assigned_to') or '-'} | {c.get('status', '?')}")
