"""
Flask Web Application – HERE GeoVerify
Fuel Station + Restaurant/Cafe Validation Engine
With Visual Validation Layer (Mapillary + YOLOv8 + OCR)
Themed for HERE Technologies
"""

from flask import Flask, request, jsonify, render_template_string
from flask_cors import CORS
from validate_station import validate, suggest
from visual_validator import validate_poi_visual

app = Flask(__name__)
CORS(app)

# ---------------------------------------------------------------------------
# HTML Template  – HERE Technologies branded design
# ---------------------------------------------------------------------------
HTML_TEMPLATE = r"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>HERE GeoVerify — Place Validation Engine</title>
    <meta name="description" content="Multi-source geospatial validation engine for fuel stations and restaurants in Singapore. Powered by HERE Technologies.">
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800;900&family=JetBrains+Mono:wght@400;500;600&display=swap" rel="stylesheet">
    <style>
        *,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
        :root {
            /* HERE brand palette */
            --here-teal: #48DAD0;
            --here-teal-dark: #00AFAA;
            --here-teal-dim: rgba(72,218,208,0.12);
            --here-teal-glow: rgba(72,218,208,0.25);
            --here-blue: #3E8BFF;
            --here-green: #6DD400;

            --bg-primary: #0B1120;
            --bg-secondary: #111D2E;
            --bg-card: rgba(17,29,46,0.75);
            --bg-card-hover: rgba(25,40,62,0.8);
            --border: rgba(72,218,208,0.10);
            --border-active: rgba(72,218,208,0.30);
            --text-primary: #E8ECF1;
            --text-secondary: #8899A6;
            --text-muted: #576879;

            --green: #6DD400;
            --green-bg: rgba(109,212,0,0.10);
            --yellow: #F5A623;
            --yellow-bg: rgba(245,166,35,0.10);
            --orange: #F76B1C;
            --orange-bg: rgba(247,107,28,0.10);
            --red: #FF4D4F;
            --red-bg: rgba(255,77,79,0.10);
            --blue: #3E8BFF;
            --blue-bg: rgba(62,139,255,0.10);

            --radius: 14px;
            --radius-sm: 10px;
            --radius-xs: 6px;
            --shadow: 0 4px 30px rgba(0,0,0,0.35);
            --transition: 0.2s cubic-bezier(0.4,0,0.2,1);
        }
        body {
            font-family:'Inter',-apple-system,BlinkMacSystemFont,sans-serif;
            background:var(--bg-primary);
            color:var(--text-primary);
            min-height:100vh;
            overflow-x:hidden;
        }

        /* Animated background grid */
        .bg-grid {
            position:fixed; top:0;left:0;right:0;bottom:0;
            background-image:
                radial-gradient(circle at 15% 25%, rgba(72,218,208,0.05) 0%, transparent 50%),
                radial-gradient(circle at 85% 75%, rgba(62,139,255,0.04) 0%, transparent 50%),
                linear-gradient(rgba(72,218,208,0.025) 1px, transparent 1px),
                linear-gradient(90deg, rgba(72,218,208,0.025) 1px, transparent 1px);
            background-size:100% 100%, 100% 100%, 50px 50px, 50px 50px;
            z-index:0; pointer-events:none;
        }

        .container { max-width:1100px; margin:0 auto; padding:1.5rem 1.5rem 4rem; position:relative; z-index:1; }

        /* ── HEADER ── */
        .header { text-align:center; margin-bottom:2rem; padding-top:1.5rem; }
        .header .logo { display:inline-flex; align-items:center; gap:0.75rem; margin-bottom:0.5rem; }
        .logo-icon {
            width:44px; height:44px;
            background:linear-gradient(135deg, var(--here-teal), var(--here-blue));
            border-radius:12px;
            display:flex; align-items:center; justify-content:center;
            font-weight:900; font-size:0.9rem; color:#0B1120;
            box-shadow:0 0 30px var(--here-teal-glow);
            letter-spacing:-0.03em;
        }
        .header h1 {
            font-size:2rem; font-weight:800;
            background:linear-gradient(135deg, var(--here-teal) 0%, var(--text-primary) 70%);
            -webkit-background-clip:text; -webkit-text-fill-color:transparent;
            background-clip:text; letter-spacing:-0.03em;
        }
        .header p { color:var(--text-secondary); font-size:0.95rem; margin-top:0.35rem; }
        .badge-row { display:flex; gap:0.4rem; justify-content:center; margin-top:0.75rem; flex-wrap:wrap; }
        .badge {
            display:inline-flex; align-items:center; gap:0.3rem;
            padding:0.25rem 0.65rem; font-size:0.68rem; font-weight:600;
            text-transform:uppercase; letter-spacing:0.06em; border-radius:100px;
            background:var(--here-teal-dim); color:var(--here-teal); border:1px solid rgba(72,218,208,0.12);
        }
        .badge .dot { width:5px; height:5px; border-radius:50%; background:var(--here-teal); }

        /* ── TYPE TOGGLE ── */
        .type-toggle {
            display:flex; gap:0; justify-content:center; margin-bottom:1.5rem;
            background:var(--bg-secondary); border-radius:var(--radius); border:1px solid var(--border);
            padding:4px; width:fit-content; margin-left:auto; margin-right:auto;
        }
        .type-btn {
            padding:0.7rem 1.8rem; border:none; background:transparent;
            color:var(--text-secondary); font-family:inherit; font-size:0.9rem; font-weight:600;
            cursor:pointer; border-radius:var(--radius-sm); transition:var(--transition);
            display:flex; align-items:center; gap:0.5rem;
        }
        .type-btn:hover { color:var(--text-primary); }
        .type-btn.active {
            background:linear-gradient(135deg, var(--here-teal-dark), rgba(72,218,208,0.25));
            color:#fff; box-shadow:0 2px 16px var(--here-teal-glow);
        }

        /* ── SEARCH CARD ── */
        .search-card {
            background:var(--bg-card); border:1px solid var(--border);
            border-radius:var(--radius); padding:1.75rem;
            backdrop-filter:blur(20px); box-shadow:var(--shadow); margin-bottom:1.5rem;
        }
        .search-card h2 { font-size:1.05rem; font-weight:700; margin-bottom:1.25rem; display:flex; align-items:center; gap:0.5rem; }
        .form-grid { display:grid; grid-template-columns:1fr 1fr 1fr; gap:0.85rem; }
        .form-group { display:flex; flex-direction:column; gap:0.35rem; position:relative; }
        .form-group.full-width { grid-column:1/-1; }
        .form-group label {
            font-size:0.72rem; font-weight:600; color:var(--text-secondary);
            text-transform:uppercase; letter-spacing:0.06em;
        }
        .form-group input {
            padding:0.7rem 0.9rem; background:rgba(11,17,32,0.85);
            border:1px solid var(--border); border-radius:var(--radius-sm);
            color:var(--text-primary); font-family:inherit; font-size:0.92rem;
            transition:var(--transition); outline:none;
        }
        .form-group input::placeholder { color:var(--text-muted); }
        .form-group input:focus { border-color:var(--here-teal); box-shadow:0 0 0 3px var(--here-teal-glow); }
        .btn-primary {
            grid-column:1/-1; padding:0.8rem 2rem;
            background:linear-gradient(135deg, var(--here-teal-dark), var(--here-teal));
            color:#0B1120; font-family:inherit; font-size:0.95rem; font-weight:700;
            border:none; border-radius:var(--radius-sm); cursor:pointer;
            transition:var(--transition); text-transform:uppercase; letter-spacing:0.08em;
            position:relative; overflow:hidden;
        }
        .btn-primary::before {
            content:''; position:absolute; top:0; left:-100%; width:100%; height:100%;
            background:linear-gradient(90deg,transparent,rgba(255,255,255,0.15),transparent);
            transition:0.5s;
        }
        .btn-primary:hover::before { left:100%; }
        .btn-primary:hover { transform:translateY(-1px); box-shadow:0 6px 24px var(--here-teal-glow); }
        .btn-primary:active { transform:translateY(0); }
        .btn-primary:disabled { opacity:0.5; cursor:not-allowed; transform:none!important; }

        /* ── AUTOCOMPLETE ── */
        .autocomplete-list {
            position:absolute; top:100%; left:0; right:0; z-index:50;
            background:var(--bg-secondary); border:1px solid var(--border-active);
            border-radius:var(--radius-sm); margin-top:4px;
            max-height:260px; overflow-y:auto; display:none;
            box-shadow:0 8px 30px rgba(0,0,0,0.4);
        }
        .autocomplete-list.open { display:block; }
        .autocomplete-item {
            padding:0.6rem 0.9rem; cursor:pointer; font-size:0.85rem;
            border-bottom:1px solid var(--border); transition:var(--transition);
            display:flex; justify-content:space-between; align-items:center;
        }
        .autocomplete-item:last-child { border-bottom:none; }
        .autocomplete-item:hover { background:var(--bg-card-hover); }
        .autocomplete-item .ac-name { font-weight:500; }
        .autocomplete-item .ac-coords { font-size:0.72rem; color:var(--text-muted); font-family:'JetBrains Mono',monospace; }

        /* ── EXAMPLES ── */
        .examples { margin-top:1rem; display:flex; flex-wrap:wrap; gap:0.4rem; align-items:center; }
        .examples span { font-size:0.72rem; color:var(--text-muted); font-weight:500; }
        .example-btn {
            background:var(--here-teal-dim); border:1px solid rgba(72,218,208,0.10);
            color:var(--here-teal); padding:0.3rem 0.7rem; border-radius:100px;
            font-size:0.72rem; font-weight:500; cursor:pointer; transition:var(--transition); font-family:inherit;
        }
        .example-btn:hover { background:rgba(72,218,208,0.18); border-color:rgba(72,218,208,0.25); }

        /* ── LOADER ── */
        .loader-wrap { display:none; flex-direction:column; align-items:center; gap:1rem; padding:2.5rem 0; }
        .loader-wrap.active { display:flex; }
        .spinner {
            width:44px; height:44px;
            border:3px solid rgba(72,218,208,0.12);
            border-top-color:var(--here-teal);
            border-radius:50%; animation:spin 0.7s linear infinite;
        }
        @keyframes spin { to { transform:rotate(360deg); } }
        .loader-wrap p { color:var(--text-secondary); font-size:0.9rem; animation:pulse 1.5s ease-in-out infinite; }
        @keyframes pulse { 0%,100%{opacity:1} 50%{opacity:0.4} }

        /* ── RESULTS ── */
        .results-wrap { display:none; }
        .results-wrap.active { display:block; }

        .decision-banner {
            border-radius:var(--radius); padding:1.75rem; text-align:center;
            margin-bottom:1.5rem; backdrop-filter:blur(20px); border:1px solid;
            position:relative; overflow:hidden;
        }
        .decision-banner::before { content:''; position:absolute; top:0;left:0;right:0; height:3px; }
        .decision-banner.confirmed { background:var(--green-bg); border-color:rgba(109,212,0,0.2); }
        .decision-banner.confirmed::before { background:var(--green); }
        .decision-banner.likely { background:var(--blue-bg); border-color:rgba(62,139,255,0.2); }
        .decision-banner.likely::before { background:var(--blue); }
        .decision-banner.uncertain { background:var(--yellow-bg); border-color:rgba(245,166,35,0.2); }
        .decision-banner.uncertain::before { background:var(--yellow); }
        .decision-banner.not-exist { background:var(--red-bg); border-color:rgba(255,77,79,0.2); }
        .decision-banner.not-exist::before { background:var(--red); }
        .decision-banner .score-big { font-size:3.5rem; font-weight:900; line-height:1; letter-spacing:-0.03em; }
        .decision-banner .decision-label { font-size:1rem; font-weight:700; letter-spacing:0.06em; text-transform:uppercase; margin-top:0.4rem; }
        .decision-banner .station-name { font-size:0.85rem; color:var(--text-secondary); margin-top:0.35rem; }

        .breakdown-strip { display:grid; gap:0.6rem; margin-bottom:1.5rem; }
        .breakdown-strip.cols-5 { grid-template-columns:repeat(5,1fr); }
        .breakdown-strip.cols-4 { grid-template-columns:repeat(4,1fr); }
        .breakdown-item {
            background:var(--bg-card); border:1px solid var(--border);
            border-radius:var(--radius-sm); padding:0.9rem; text-align:center;
            backdrop-filter:blur(12px); transition:var(--transition);
        }
        .breakdown-item:hover { border-color:var(--border-active); transform:translateY(-2px); }
        .breakdown-item .bi-label { font-size:0.65rem; font-weight:600; color:var(--text-muted); text-transform:uppercase; letter-spacing:0.08em; margin-bottom:0.4rem; }
        .breakdown-item .bi-score { font-size:1.4rem; font-weight:800; font-family:'JetBrains Mono',monospace; }
        .breakdown-item .bi-weight { font-size:0.62rem; color:var(--text-muted); margin-top:0.2rem; }

        .detail-grid { display:grid; grid-template-columns:1fr 1fr; gap:0.85rem; margin-bottom:0.85rem; }
        .detail-card {
            background:var(--bg-card); border:1px solid var(--border);
            border-radius:var(--radius); padding:1.25rem;
            backdrop-filter:blur(12px); transition:var(--transition);
        }
        .detail-card:hover { border-color:var(--border-active); }
        .detail-card .dc-header { display:flex; align-items:center; justify-content:space-between; margin-bottom:0.85rem; }
        .dc-header .dc-title { font-size:0.82rem; font-weight:700; display:flex; align-items:center; gap:0.4rem; }
        .dc-header .dc-badge {
            font-size:0.65rem; font-weight:700; padding:0.18rem 0.55rem;
            border-radius:100px; text-transform:uppercase; letter-spacing:0.05em;
        }
        .dc-badge.match { background:var(--green-bg); color:var(--green); }
        .dc-badge.no-match { background:var(--red-bg); color:var(--red); }
        .dc-badge.active { background:var(--green-bg); color:var(--green); }
        .dc-badge.closed { background:var(--red-bg); color:var(--red); }
        .dc-badge.unknown { background:var(--yellow-bg); color:var(--yellow); }
        .dc-rows { display:flex; flex-direction:column; gap:0.5rem; }
        .dc-row { display:flex; justify-content:space-between; align-items:center; font-size:0.8rem; }
        .dc-row .dc-key { color:var(--text-muted); font-weight:500; }
        .dc-row .dc-val { color:var(--text-primary); font-weight:600; font-family:'JetBrains Mono',monospace; font-size:0.78rem; text-align:right; max-width:60%; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }

        .json-panel { background:var(--bg-card); border:1px solid var(--border); border-radius:var(--radius); overflow:hidden; margin-top:0.85rem; }
        .json-header {
            display:flex; align-items:center; justify-content:space-between;
            padding:0.85rem 1.25rem; border-bottom:1px solid var(--border);
            cursor:pointer; user-select:none; transition:var(--transition);
        }
        .json-header:hover { background:var(--bg-card-hover); }
        .json-header span { font-size:0.82rem; font-weight:700; }
        .json-header .toggle-icon { transition:transform 0.3s; font-size:0.85rem; color:var(--text-muted); }
        .json-header.open .toggle-icon { transform:rotate(180deg); }
        .json-body { max-height:0; overflow:hidden; transition:max-height 0.4s ease; }
        .json-body.open { max-height:3000px; }
        .json-body pre { padding:1rem 1.25rem; font-family:'JetBrains Mono',monospace; font-size:0.75rem; line-height:1.7; color:var(--text-secondary); overflow-x:auto; }

        .score-high { color:var(--green); }
        .score-mid { color:var(--yellow); }
        .score-low { color:var(--orange); }
        .score-zero { color:var(--red); }

        .fade-in { animation:fadeIn 0.4s ease forwards; }
        @keyframes fadeIn { from{opacity:0;transform:translateY(10px)} to{opacity:1;transform:translateY(0)} }

        .footer { text-align:center; padding:2rem 0 1rem; color:var(--text-muted); font-size:0.72rem; }
        .footer a { color:var(--here-teal); text-decoration:none; }

        @media(max-width:768px){
            .form-grid{grid-template-columns:1fr}
            .breakdown-strip.cols-5,.breakdown-strip.cols-4{grid-template-columns:repeat(2,1fr)}
            .detail-grid{grid-template-columns:1fr}
            .header h1{font-size:1.6rem}
            .type-btn{padding:0.6rem 1.2rem; font-size:0.82rem}
        }
    </style>
</head>
<body>
    <div class="bg-grid"></div>
    <div class="container">
        <!-- Header -->
        <header class="header">
            <div class="logo">
                <div class="logo-icon">HERE</div>
                <h1>GeoVerify</h1>
            </div>
            <p>Multi-source geospatial validation engine for Singapore</p>
            <div class="badge-row">
                <span class="badge"><span class="dot"></span>OSM</span>
                <span class="badge"><span class="dot"></span>ACRA</span>
                <span class="badge"><span class="dot"></span>Overture</span>
                <span class="badge"><span class="dot"></span>Spatial</span>
                <span class="badge"><span class="dot"></span>Brand</span>
                <span class="badge" style="background:rgba(62,139,255,0.12);color:var(--here-blue);border-color:rgba(62,139,255,0.15)"><span class="dot" style="background:var(--here-blue)"></span>Visual AI</span>
            </div>
        </header>

        <!-- Type Toggle -->
        <div class="type-toggle" id="typeToggle">
            <button class="type-btn active" data-type="fuel_station" onclick="switchType('fuel_station')">⛽ Fuel Station</button>
            <button class="type-btn" data-type="restaurant" onclick="switchType('restaurant')">🍽️ Restaurant / Cafe</button>
        </div>

        <!-- Search Card -->
        <section class="search-card">
            <h2 id="searchTitle">🔍 Validate Place</h2>
            <form id="validateForm" class="form-grid" autocomplete="off">
                <div class="form-group full-width">
                    <label for="placeName">Place Name</label>
                    <input type="text" id="placeName" placeholder="Start typing to search..." required>
                    <div class="autocomplete-list" id="autocompleteList"></div>
                </div>
                <div class="form-group">
                    <label for="lat">Latitude</label>
                    <input type="number" id="lat" step="any" placeholder="1.3521">
                </div>
                <div class="form-group">
                    <label for="lon">Longitude</label>
                    <input type="number" id="lon" step="any" placeholder="103.8198">
                </div>
                <div class="form-group" style="justify-content:flex-end">
                    <button type="submit" class="btn-primary" id="submitBtn">▶ Validate</button>
                </div>
            </form>
            <div class="examples" id="examples"></div>
        </section>

        <div class="loader-wrap" id="loader">
            <div class="spinner"></div>
            <p id="loaderText">Validating across data sources...</p>
        </div>

        <div class="results-wrap" id="results"></div>

        <div class="footer">
            Powered by <a href="https://www.here.com" target="_blank">HERE Technologies</a> · OSM · ACRA · Overture Maps
        </div>
    </div>

    <script>
    /* ── State ── */
    let currentType = 'fuel_station';
    let acTimeout = null;

    const EXAMPLES = {
        fuel_station: [
            {name:'Shell Bukit Timah', lat:'1.33942', lon:'103.77661'},
            {name:'Esso West Coast', lat:'1.30498', lon:'103.76431'},
            {name:'SPC Tampines', lat:'1.35360', lon:'103.94400'},
            {name:'Caltex Yishun', lat:'1.42950', lon:'103.83530'},
            {name:'Zqwx Nonexistent Fuel', lat:'1.35000', lon:'103.85000'},
        ],
        restaurant: [
            {name:'Starbucks', lat:'1.28967', lon:'103.85007'},
            {name:'McDonald\'s', lat:'1.30060', lon:'103.83760'},
            {name:'Soup Restaurant', lat:'1.34007', lon:'103.70656'},
            {name:'Ya Kun Kaya Toast', lat:'1.28210', lon:'103.85050'},
            {name:'Zqwx Nonexistent Diner', lat:'1.35000', lon:'103.85000'},
        ]
    };

    /* ── Type switching ── */
    function switchType(type) {
        currentType = type;
        document.querySelectorAll('.type-btn').forEach(b => {
            b.classList.toggle('active', b.dataset.type === type);
        });
        document.getElementById('results').classList.remove('active');
        document.getElementById('results').innerHTML = '';
        document.getElementById('placeName').value = '';
        document.getElementById('lat').value = '';
        document.getElementById('lon').value = '';
        document.getElementById('placeName').placeholder =
            type === 'fuel_station' ? 'e.g. Shell Bukit Timah' : 'e.g. Starbucks Raffles Place';
        renderExamples();
    }

    function renderExamples() {
        const wrap = document.getElementById('examples');
        const exs = EXAMPLES[currentType] || [];
        wrap.innerHTML = '<span>Try:</span>' + exs.map(e =>
            `<button class="example-btn" type="button" onclick="fillExample('${e.name.replace(/'/g,"\\'")}','${e.lat}','${e.lon}')">${e.name}</button>`
        ).join('');
    }

    function fillExample(name, lat, lon) {
        document.getElementById('placeName').value = name;
        document.getElementById('lat').value = lat;
        document.getElementById('lon').value = lon;
        closeAutocomplete();
    }

    /* ── Autocomplete ── */
    const nameInput = document.getElementById('placeName');
    const acList = document.getElementById('autocompleteList');

    nameInput.addEventListener('input', () => {
        clearTimeout(acTimeout);
        const q = nameInput.value.trim();
        if (q.length < 2) { closeAutocomplete(); return; }
        acTimeout = setTimeout(() => fetchSuggestions(q), 250);
    });

    nameInput.addEventListener('blur', () => setTimeout(closeAutocomplete, 200));

    async function fetchSuggestions(q) {
        try {
            const resp = await fetch(`/api/suggest?q=${encodeURIComponent(q)}&type=${currentType}`);
            const data = await resp.json();
            if (!data.length) { closeAutocomplete(); return; }
            acList.innerHTML = data.map(s => `
                <div class="autocomplete-item" onmousedown="selectSuggestion('${s.name.replace(/'/g,"\\'")}',${s.lat},${s.lon})">
                    <span class="ac-name">${s.name}</span>
                    <span class="ac-coords">${s.lat}, ${s.lon}</span>
                </div>
            `).join('');
            acList.classList.add('open');
        } catch(e) { closeAutocomplete(); }
    }

    function selectSuggestion(name, lat, lon) {
        nameInput.value = name;
        document.getElementById('lat').value = lat;
        document.getElementById('lon').value = lon;
        closeAutocomplete();
    }

    function closeAutocomplete() { acList.classList.remove('open'); acList.innerHTML = ''; }

    /* ── Helpers ── */
    function scoreColor(v, max) {
        const pct = (v / max) * 100;
        if (pct >= 70) return 'score-high';
        if (pct >= 40) return 'score-mid';
        if (pct > 0)  return 'score-low';
        return 'score-zero';
    }
    function decisionClass(d) {
        if (d.includes('CONFIRMED')) return 'confirmed';
        if (d.includes('LIKELY'))    return 'likely';
        if (d.includes('UNCERTAIN')) return 'uncertain';
        return 'not-exist';
    }
    function decisionEmoji(d) {
        if (d.includes('CONFIRMED')) return '✅';
        if (d.includes('LIKELY'))    return '🔵';
        if (d.includes('UNCERTAIN')) return '⚠️';
        return '❌';
    }
    function fmt(v) { return v != null && v !== '' ? v : '—'; }
    function fmtS(v) { return v != null ? v.toFixed(2) : '0.00'; }

    /* ── Render Results ── */
    function renderResults(data) {
        const cls = decisionClass(data.final.decision);
        const emoji = decisionEmoji(data.final.decision);
        const isFuel = data.input.place_type === 'fuel_station';
        const typeLabel = isFuel ? '⛽ Fuel Station' : '🍽️ Restaurant';

        // Weights (with visual layer)
        const w = isFuel
            ? {osm:0.25, acra:0.20, overture:0.17, spatial:0.13, brand:0.10, visual:0.15}
            : {osm:0.28, acra:0.25, overture:0.20, brand:0.10, visual:0.17};

        const visualConf = data.visual?.confidence || 0;
        const visualScore01 = visualConf / 100;

        let breakdownHtml = '';
        breakdownHtml += bi('OSM', data.osm.score, w.osm);
        breakdownHtml += bi('ACRA', data.acra.score, w.acra);
        breakdownHtml += bi('Overture', data.overture.score, w.overture);
        if (isFuel) breakdownHtml += bi('Spatial', data.spatial?.road_proximity_score || 0, w.spatial);
        breakdownHtml += bi('Brand', data.brand.consistency_score, w.brand);
        breakdownHtml += bi('Visual', visualScore01, w.visual);

        function bi(label, score, weight) {
            return `<div class="breakdown-item">
                <div class="bi-label">${label}</div>
                <div class="bi-score ${scoreColor(score,1)}">${fmtS(score)}</div>
                <div class="bi-weight">×${weight.toFixed(2)}</div>
            </div>`;
        }

        let html = `
        <div class="decision-banner ${cls} fade-in">
            <div class="score-big ${scoreColor(data.final.score,100)}">${data.final.score}</div>
            <div class="decision-label">${emoji} ${data.final.decision}</div>
            <div class="station-name">${typeLabel} · ${data.input.name} ${data.input.lat ? `(${data.input.lat}, ${data.input.lon})` : '(no coords)'}</div>
        </div>

        <div class="breakdown-strip ${isFuel?'cols-5':'cols-4'}" style="display:grid;grid-template-columns:repeat(auto-fit,minmax(90px,1fr));gap:0.6rem;margin-bottom:1.5rem;animation:fadeIn 0.4s ease forwards;animation-delay:.08s;opacity:0">
            ${breakdownHtml}
        </div>

        <div class="detail-grid fade-in" style="animation-delay:.16s">
            <!-- OSM -->
            <div class="detail-card">
                <div class="dc-header">
                    <span class="dc-title">🗺️ OpenStreetMap</span>
                    <span class="dc-badge ${data.osm.match?'match':'no-match'}">${data.osm.match?'MATCH':'NO MATCH'}</span>
                </div>
                <div class="dc-rows">${data.osm.details ? `
                    <div class="dc-row"><span class="dc-key">Name</span><span class="dc-val">${fmt(data.osm.details.name)}</span></div>
                    <div class="dc-row"><span class="dc-key">Brand</span><span class="dc-val">${fmt(data.osm.details.brand)}</span></div>
                    <div class="dc-row"><span class="dc-key">Distance</span><span class="dc-val">${data.osm.details.distance_m!=null?data.osm.details.distance_m+'m':'—'}</span></div>
                    <div class="dc-row"><span class="dc-key">Similarity</span><span class="dc-val">${fmtS(data.osm.details.name_similarity)}</span></div>
                ` : '<div class="dc-row"><span class="dc-key">No match found</span></div>'}</div>
            </div>

            <!-- ACRA -->
            <div class="detail-card">
                <div class="dc-header">
                    <span class="dc-title">🏢 ACRA Registry</span>
                    <span class="dc-badge ${data.acra.match?(data.acra.status==='active'?'active':data.acra.status==='closed'?'closed':'unknown'):'no-match'}">${data.acra.match?data.acra.status.toUpperCase():'NO MATCH'}</span>
                </div>
                <div class="dc-rows">${data.acra.details ? `
                    <div class="dc-row"><span class="dc-key">${isFuel?'Building':'Name'}</span><span class="dc-val">${fmt(data.acra.details.building || data.acra.details.name)}</span></div>
                    <div class="dc-row"><span class="dc-key">Street</span><span class="dc-val">${fmt(data.acra.details.street)}</span></div>
                    <div class="dc-row"><span class="dc-key">Postal</span><span class="dc-val">${fmt(data.acra.details.postal_code)}</span></div>
                    <div class="dc-row"><span class="dc-key">Brand</span><span class="dc-val">${fmt(data.acra.details.brand)}</span></div>
                ` : '<div class="dc-row"><span class="dc-key">No match found</span></div>'}</div>
            </div>

            <!-- Overture -->
            <div class="detail-card">
                <div class="dc-header">
                    <span class="dc-title">🌐 Overture Maps</span>
                    <span class="dc-badge ${data.overture.match?'match':'no-match'}">${data.overture.match?'MATCH':'NO MATCH'}</span>
                </div>
                <div class="dc-rows">${data.overture.details && !data.overture.details.error ? `
                    <div class="dc-row"><span class="dc-key">Name</span><span class="dc-val">${fmt(data.overture.details.name)}</span></div>
                    <div class="dc-row"><span class="dc-key">Brand</span><span class="dc-val">${fmt(data.overture.details.brand)}</span></div>
                    <div class="dc-row"><span class="dc-key">Distance</span><span class="dc-val">${data.overture.details.distance_m!=null?data.overture.details.distance_m+'m':'—'}</span></div>
                    <div class="dc-row"><span class="dc-key">Status</span><span class="dc-val">${fmt(data.overture.details.operating_status)}</span></div>
                ` : `<div class="dc-row"><span class="dc-key">${data.overture.details?.error||'No match found'}</span></div>`}</div>
            </div>

            <!-- Spatial (fuel only) OR Brand (restaurant) -->
            ${isFuel ? `
            <div class="detail-card">
                <div class="dc-header">
                    <span class="dc-title">🛣️ Road Proximity</span>
                    <span class="dc-badge ${(data.spatial?.road_proximity_score||0)>=0.7?'match':(data.spatial?.road_proximity_score||0)>0?'unknown':'no-match'}">${(data.spatial?.road_proximity_score||0)>=0.7?'NEAR ROAD':(data.spatial?.road_proximity_score||0)>0?'MODERATE':'FAR'}</span>
                </div>
                <div class="dc-rows">${data.spatial?.details ? `
                    <div class="dc-row"><span class="dc-key">Distance</span><span class="dc-val">${fmt(data.spatial.details.nearest_road_distance_m)}m</span></div>
                    <div class="dc-row"><span class="dc-key">Road Type</span><span class="dc-val">${fmt(data.spatial.details.nearest_road_type)}</span></div>
                ` : '<div class="dc-row"><span class="dc-key">No spatial data</span></div>'}</div>
            </div>
            ` : `
            <div class="detail-card">
                <div class="dc-header">
                    <span class="dc-title">🏷️ Brand Consistency</span>
                    <span class="dc-badge ${data.brand.consistency_score>=1?'match':data.brand.consistency_score>0?'unknown':'no-match'}">${data.brand.consistency_score>=1?'CONSISTENT':data.brand.consistency_score>0?'PARTIAL':'NONE'}</span>
                </div>
                <div class="dc-rows">
                    ${data.brand.details?.brands ? Object.entries(data.brand.details.brands).map(([src,brand])=>
                        `<div class="dc-row"><span class="dc-key">${src.toUpperCase()}</span><span class="dc-val">${brand}</span></div>`
                    ).join('') : ''}
                    <div class="dc-row"><span class="dc-key">Note</span><span class="dc-val">${data.brand.details?.note||'—'}</span></div>
                </div>
            </div>
            `}
        </div>

        ${isFuel ? `
        <!-- Brand card for fuel (separate row) -->
        <div class="detail-card fade-in" style="animation-delay:.24s; margin-bottom:0.85rem;">
            <div class="dc-header">
                <span class="dc-title">🏷️ Brand Consistency</span>
                <span class="dc-badge ${data.brand.consistency_score>=1?'match':data.brand.consistency_score>0?'unknown':'no-match'}">${data.brand.consistency_score>=1?'CONSISTENT':data.brand.consistency_score>0?'PARTIAL':'NONE'}</span>
            </div>
            <div class="dc-rows">
                ${data.brand.details?.brands ? Object.entries(data.brand.details.brands).map(([src,brand])=>
                    `<div class="dc-row"><span class="dc-key">${src.toUpperCase()}</span><span class="dc-val">${brand}</span></div>`
                ).join('') : ''}
                <div class="dc-row"><span class="dc-key">Note</span><span class="dc-val">${data.brand.details?.note||'—'}</span></div>
            </div>
        </div>
        ` : ''}

        <!-- Visual Validation Panel -->
        ${data.visual ? `
        <div class="detail-card fade-in" style="animation-delay:.32s; margin-bottom:0.85rem; border-color:rgba(62,139,255,0.2); grid-column:1/-1;">
            <div class="dc-header">
                <span class="dc-title">🔍 Visual Validation (Mapillary + YOLOv8 + OCR)</span>
                <span class="dc-badge ${data.visual.status==='open'?'match':data.visual.status==='closed'||data.visual.status==='relocated'?'closed':data.visual.status==='under_construction'?'unknown':'no-match'}">
                    ${data.visual.status?.toUpperCase() || 'UNCERTAIN'}
                </span>
            </div>
            <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:1rem;margin-bottom:1rem">
                <div style="text-align:center;padding:0.75rem;background:rgba(62,139,255,0.06);border-radius:10px">
                    <div style="font-size:0.65rem;text-transform:uppercase;color:var(--text-muted);font-weight:600;letter-spacing:0.06em">Confidence</div>
                    <div style="font-size:1.6rem;font-weight:800;font-family:'JetBrains Mono',monospace" class="${scoreColor(data.visual.confidence||0,100)}">${data.visual.confidence || 0}</div>
                </div>
                <div style="text-align:center;padding:0.75rem;background:rgba(62,139,255,0.06);border-radius:10px">
                    <div style="font-size:0.65rem;text-transform:uppercase;color:var(--text-muted);font-weight:600;letter-spacing:0.06em">Evidence Score</div>
                    <div style="font-size:1.6rem;font-weight:800;font-family:'JetBrains Mono',monospace" class="${scoreColor(data.visual.visual_evidence_score||0,100)}">${data.visual.visual_evidence_score || 0}</div>
                </div>
                <div style="text-align:center;padding:0.75rem;background:rgba(62,139,255,0.06);border-radius:10px">
                    <div style="font-size:0.65rem;text-transform:uppercase;color:var(--text-muted);font-weight:600;letter-spacing:0.06em">Decision Hint</div>
                    <div style="font-size:0.85rem;font-weight:700;margin-top:0.3rem;color:${
                        data.visual.final_decision_hint==='strong_positive'?'var(--green)':
                        data.visual.final_decision_hint==='weak_positive'?'var(--blue)':
                        data.visual.final_decision_hint==='strong_negative'?'var(--red)':
                        data.visual.final_decision_hint==='weak_negative'?'var(--orange)':'var(--text-secondary)'
                    }">${(data.visual.final_decision_hint||'neutral').replace(/_/g,' ').toUpperCase()}</div>
                </div>
            </div>
            <div class="dc-rows">
                <div class="dc-row"><span class="dc-key">Images Analysed</span><span class="dc-val">${data.visual.images_analysed || 0}</span></div>
                <div class="dc-row"><span class="dc-key">Activity Detected</span><span class="dc-val">${data.visual.signals?.activity ? '✅ Yes' : '❌ No'}</span></div>
                <div class="dc-row"><span class="dc-key">Open Sign</span><span class="dc-val">${data.visual.signals?.open_sign ? '✅ Yes' : '❌ No'}</span></div>
                <div class="dc-row"><span class="dc-key">Closed Sign</span><span class="dc-val">${data.visual.signals?.closed_sign ? '✅ Yes' : '❌ No'}</span></div>
                <div class="dc-row"><span class="dc-key">Construction</span><span class="dc-val">${data.visual.signals?.construction ? '⚠️ Yes' : '❌ No'}</span></div>
                <div class="dc-row"><span class="dc-key">Relocation</span><span class="dc-val">${data.visual.signals?.relocation ? '⚠️ Yes' : '❌ No'}</span></div>
                <div class="dc-row"><span class="dc-key">Detected Objects</span><span class="dc-val" style="max-width:70%">${(data.visual.signals?.detected_objects||[]).join(', ') || '—'}</span></div>
                <div class="dc-row"><span class="dc-key">OCR Text</span><span class="dc-val" style="max-width:70%;white-space:normal;word-break:break-word">${data.visual.signals?.ocr_text || '—'}</span></div>
                ${data.visual.reason ? `<div class="dc-row"><span class="dc-key">Note</span><span class="dc-val" style="color:var(--yellow)">${data.visual.reason}</span></div>` : ''}
            </div>
        </div>
        ` : ''}

        <!-- JSON -->
        <div class="json-panel fade-in" style="animation-delay:.3s">
            <div class="json-header" onclick="toggleJson(this)">
                <span>📋 Raw JSON Response</span>
                <span class="toggle-icon">▼</span>
            </div>
            <div class="json-body">
                <pre>${JSON.stringify(data, null, 2)}</pre>
            </div>
        </div>`;

        document.getElementById('results').innerHTML = html;
        document.getElementById('results').classList.add('active');
    }

    function toggleJson(el) { el.classList.toggle('open'); el.nextElementSibling.classList.toggle('open'); }

    /* ── Submit ── */
    document.getElementById('validateForm').addEventListener('submit', async (e) => {
        e.preventDefault();
        const name = document.getElementById('placeName').value.trim();
        const lat  = document.getElementById('lat').value || null;
        const lon  = document.getElementById('lon').value || null;
        if (!name) return;

        const btn = document.getElementById('submitBtn');
        btn.disabled = true; btn.textContent = '⏳ Validating...';
        document.getElementById('results').classList.remove('active');
        document.getElementById('loader').classList.add('active');
        document.getElementById('loaderText').textContent =
            `Validating ${currentType === 'fuel_station' ? 'fuel station' : 'restaurant'} across ${currentType === 'fuel_station' ? '5' : '4'} data sources + visual AI...`;

        try {
            const body = { name, place_type: currentType };
            if (lat) body.lat = parseFloat(lat);
            if (lon) body.lon = parseFloat(lon);

            const resp = await fetch('/api/validate', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(body),
            });
            const data = await resp.json();
            renderResults(data);
        } catch (err) {
            document.getElementById('results').innerHTML = `
                <div class="decision-banner not-exist fade-in">
                    <div class="score-big score-zero">ERR</div>
                    <div class="decision-label">⚠️ Validation Error</div>
                    <div class="station-name">${err.message}</div>
                </div>`;
            document.getElementById('results').classList.add('active');
        } finally {
            document.getElementById('loader').classList.remove('active');
            btn.disabled = false; btn.textContent = '▶ Validate';
        }
    });

    /* ── Init ── */
    renderExamples();
    </script>
</body>
</html>
"""


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    return render_template_string(HTML_TEMPLATE)


@app.route("/api/validate", methods=["POST"])
def api_validate():
    payload = request.get_json(silent=True) or {}
    name = payload.get("name", "").strip()
    if not name:
        return jsonify({"error": "name is required"}), 400

    lat = payload.get("lat")
    lon = payload.get("lon")
    if lat is not None:
        lat = float(lat)
    if lon is not None:
        lon = float(lon)

    # Allow disabling visual validation via query param or payload
    run_visual = payload.get("run_visual", True)

    place_type = payload.get("place_type", "fuel_station")
    if place_type not in ("fuel_station", "restaurant"):
        place_type = "fuel_station"

    result = validate(name, lat, lon, place_type, run_visual=run_visual)
    return jsonify(result)


@app.route("/api/suggest")
def api_suggest():
    q = request.args.get("q", "").strip()
    place_type = request.args.get("type", "fuel_station")
    if place_type not in ("fuel_station", "restaurant"):
        place_type = "fuel_station"
    results = suggest(q, place_type)
    return jsonify(results)


@app.route("/api/visual-validate", methods=["POST"])
def api_visual_validate():
    """Standalone visual validation endpoint.
    Accepts the full visual validation input payload directly."""
    payload = request.get_json(silent=True) or {}

    lat = payload.get("latitude")
    lon = payload.get("longitude")
    category = payload.get("category", "fuel_station")
    poi_name = payload.get("poi_name", "")

    if lat is None or lon is None:
        return jsonify({"error": "latitude and longitude are required"}), 400

    if category not in ("fuel_station", "restaurant"):
        category = "fuel_station"

    upstream = payload.get("upstream_signals", {
        "acra_exists": False,
        "osm_exists": False,
        "overture_exists": False,
        "brand_match": False,
    })

    result = validate_poi_visual({
        "latitude": float(lat),
        "longitude": float(lon),
        "category": category,
        "poi_name": poi_name,
        "upstream_signals": upstream,
    })
    return jsonify(result)


if __name__ == "__main__":
    print("Starting HERE GeoVerify server ...")
    app.run(host="0.0.0.0", port=5000, debug=True, use_reloader=False)
