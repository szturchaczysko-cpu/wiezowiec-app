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
# Hardcoded + custom z bazy
WIEZOWIEC_PROMPT_URLS = {
    "Wieżowiec v3 (stabilny)": "https://raw.githubusercontent.com/szturchaczysko-cpu/szturchacz/refs/heads/main/prompt_wiezowiec_v3_pelny.md",
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
# PARSER WYJŚCIA WIEŻOWCA
# ==========================================
def parse_wiezowiec_output(text):
    """
    Parsuje surowy wynik AI Wieżowca na listę casów.
    
    Rozpoznaje:
    - Nagłówki grup: ▬▬▬ OPERATORZY DE (XX zamówień) ▬▬▬
    - Nagłówki priorytetowe: 🔴 [145] | B-KRYTYCZNY | Index: ...
    - Dodatkowe linie pod nagłówkiem (⏰ KOTWICA itp.)
    - Pełne linie szturchacza aż do separatora ---
    """
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

        # Nagłówek grupy?
        for grupa, pattern in grupa_patterns.items():
            if re.search(pattern, line):
                current_grupa = grupa
                break

        # Nagłówek priorytetowy? (ikona + [score])
        icon_match = re.match(r'^([🔴🟡🟢⚪📦])\s*\[(\d+)\]\s*\|\s*(.*)', line)
        if icon_match and current_grupa:
            icon = icon_match.group(1)
            score = int(icon_match.group(2))
            label = icon_match.group(3).strip()
            naglowek = line

            # Zbierz linie bloku aż do separatora ---
            i += 1
            blok_lines = []
            while i < len(lines):
                nl = lines[i].strip()
                if nl == '---' or nl.startswith('▬') or nl.startswith('═══'):
                    break
                if re.match(r'^[🔴🟡🟢⚪📦]\s*\[\d+\]', nl):
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

            # Index handlowy
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

        # Sekcja ALERT — pomijaj (nie są to casy do obróbki)
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
st.caption("Generator priorytetów — zapisuje casy do bazy dla operatorów")

tab_generate, tab_batches, tab_cases = st.tabs([
    "⚡ Generuj nową partię",
    "📦 Historia partii",
    "📋 Przegląd casów"
])

# ==========================================
# ⚡ GENEROWANIE
# ==========================================
with tab_generate:
    st.subheader("⚡ Nowa partia priorytetów")

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
    st.markdown("### 📥 Wklej 3 wsady")
    c1, c2, c3 = st.columns(3)
    with c1:
        wsad_swinka = st.text_area("🐷 WSAD 1: ŚWINKA", height=300)
    with c2:
        wsad_szturchacz = st.text_area("📋 WSAD 2: SZTURCHACZ", height=300)
    with c3:
        wsad_uszki = st.text_area("📦 WSAD 3: STANY USZKÓW", height=300)

    st.markdown("---")

    if st.button("🚀 Generuj priorytety", type="primary", disabled=not (wsad_swinka and wsad_szturchacz)):
        if not current_project:
            st.error("Brak projektu GCP!")
            st.stop()

        WIEZOWIEC_PROMPT = get_remote_prompt(sel_prompt_url)
        if not WIEZOWIEC_PROMPT:
            st.error("Nie udało się pobrać promptu!")
            st.stop()

        # Inicjalizacja Vertex AI
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

=== WSAD 1: ŚWINKA ===
{wsad_swinka}

=== WSAD 2: SZTURCHACZ ===
{wsad_szturchacz}

=== WSAD 3: STANY USZKÓW ===
{wsad_uszki if wsad_uszki else '(brak danych)'}
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

        # Surowy wynik
        with st.expander("📄 Surowy wynik AI", expanded=False):
            st.text(ai_text)

        # Parse
        cases = parse_wiezowiec_output(ai_text)
        if not cases:
            st.warning("⚠️ Parser nie znalazł casów. Sprawdź surowy wynik.")
            st.stop()

        de = [c for c in cases if c["grupa"] == "DE"]
        fr = [c for c in cases if c["grupa"] == "FR"]
        ukpl = [c for c in cases if c["grupa"] == "UKPL"]
        st.success(f"✅ **{len(cases)}** casów: DE={len(de)} | FR={len(fr)} | UKPL={len(ukpl)}")

        # Podgląd
        for col, label, grp in [(st.columns(3)[0], "🇩🇪 DE", de), (st.columns(3)[1], "🇫🇷 FR", fr), (st.columns(3)[2], "🇬🇧 UKPL", ukpl)]:
            pass
        pc1, pc2, pc3 = st.columns(3)
        for col, flag, grp in [(pc1, "🇩🇪 DE", de), (pc2, "🇫🇷 FR", fr), (pc3, "🇬🇧 UKPL", ukpl)]:
            with col:
                st.markdown(f"**{flag} ({len(grp)})**")
                for c in grp[:5]:
                    st.caption(f"{c['priority_icon']} [{c['score']}] {c['numer_zamowienia']}")
                if len(grp) > 5:
                    st.caption(f"...+{len(grp)-5} więcej")

        # Zapisz wynik do session_state żeby przycisk "Zapisz" nie stracił danych
        st.session_state["_ew_parsed_cases"] = cases
        st.session_state["_ew_ai_text"] = ai_text
        st.session_state["_ew_prompt_name"] = sel_prompt
        st.session_state["_ew_model"] = model_choice

    # Przycisk zapisu (osobny od generowania)
    if "_ew_parsed_cases" in st.session_state and st.session_state["_ew_parsed_cases"]:
        st.markdown("---")
        cases = st.session_state["_ew_parsed_cases"]
        if st.button("💾 Zapisz do bazy i udostępnij operatorom", type="primary"):
            tz_pl = pytz.timezone('Europe/Warsaw')
            now = datetime.now(tz_pl)
            batch_id = f"batch_{now.strftime('%Y%m%d_%H%M%S')}"

            de = [c for c in cases if c["grupa"] == "DE"]
            fr = [c for c in cases if c["grupa"] == "FR"]
            ukpl = [c for c in cases if c["grupa"] == "UKPL"]

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
                    "started_at": None,
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
                    sc = {"wolny": 0, "przydzielony": 0, "w_toku": 0, "zakonczony": 0, "pominiety": 0}
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
            smap = {"wolny": "🔵", "przydzielony": "🟡", "w_toku": "🟠", "zakonczony": "🟢", "pominiety": "⚪"}
            si = smap.get(c.get("status"), "❓")
            cc1, cc2 = st.columns([4, 1])
            with cc1:
                st.markdown(f"{si} **{c.get('numer_zamowienia', '?')}** — {c.get('priority_icon', '')} [{c.get('score', 0)}] {c.get('priority_label', '')}")
            with cc2:
                st.caption(f"{c.get('grupa', '?')} | {c.get('assigned_to') or '-'} | {c.get('status', '?')}")
