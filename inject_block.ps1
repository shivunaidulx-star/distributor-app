$file = "C:\Users\Admin\.gemini\antigravity\scratch\distributor-app\app.js"
$lines = [System.IO.File]::ReadAllLines($file)

$newLines = @()
foreach ($line in $lines) {
    $newLines += $line
    if ($line.Trim() -eq "$('sidebar-brand').textContent = co.name || 'DistroManager';") {
        $newLines += @"
    const sidebarLogo = document.querySelector('#sidebar .logo-icon-sm');
    if (sidebarLogo) {
        if (co.logo) {
            sidebarLogo.innerHTML = `<img src="`$(`$co.logo)" style="width:100%;height:100%;object-fit:cover;border-radius:6px">`;
            sidebarLogo.style.background = 'transparent';
        } else {
            sidebarLogo.textContent = (co.name || 'D').charAt(0).toUpperCase();
            sidebarLogo.style.background = 'linear-gradient(135deg, var(--primary), var(--secondary))';
        }
    }
    `$('current-date').textContent = new Date().toLocaleDateString('en-IN', { weekday: 'short', day: '2-digit', month: 'short', year: 'numeric' });
    buildSidebar();
    navigateTo('dashboard');
}

function logout() { currentUser = null; `$('login-pin').value = ''; showLoginScreen(); }

// --- Sidebar ---
function buildSidebar() {
    const pages = ROLE_PAGES[currentUser.role] || [];
    document.querySelectorAll('.sidebar-nav .nav-item').forEach(el => {
        el.style.display = pages.includes(el.dataset.page) ? 'flex' : 'none';
    });
    const divider = document.querySelector('.nav-divider');
    if (divider) {
        const hasAny = ['packers', 'deliverypersons', 'users', 'setup'].some(p => pages.includes(p));
        divider.style.display = hasAny ? 'block' : 'none';
    }
}

// --- Event Listeners ---
document.addEventListener('DOMContentLoaded', () => {
    checkFirstLaunch();
    `$('btn-login').addEventListener('click', login);
    `$('login-pin').addEventListener('keypress', e => { if (e.key === 'Enter') login(); });
    `$('btn-logout').addEventListener('click', logout);
    `$('sidebar-close').addEventListener('click', () => sidebar.classList.remove('open'));
    `$('sidebar-toggle').addEventListener('click', () => sidebar.classList.toggle('open'));
    `$('modal-close').addEventListener('click', closeModal);
    `$('modal-overlay').addEventListener('click', e => { if (e.target === `$('modal-overlay')) closeModal(); });
    document.querySelectorAll('.nav-item').forEach(item => {
        item.addEventListener('click', e => { e.preventDefault(); navigateTo(item.dataset.page); sidebar.classList.remove('open'); });
    });
});

// --- Modal ---
function openModal(title, html) { `$('modal-title').textContent = title; `$('modal-body').innerHTML = html; `$('modal-overlay').classList.remove('hidden'); }
function closeModal() { `$('modal-overlay').classList.add('hidden'); document.querySelectorAll('.search-dropdown-list').forEach(d => d.remove()); }

// --- Custom Searchable Dropdown (replaces broken datalist inside modals) ---
function initSearchDropdown(inputId, items, onSelect) {
    const inp = `$(inputId);
    if (!inp) return;

    // Create dropdown container
    let dd = document.getElementById(inputId + '-dropdown');
    if (dd) dd.remove();
    dd = document.createElement('div');
    dd.id = inputId + '-dropdown';
    dd.className = 'search-dropdown-list';
    document.body.appendChild(dd);

    let highlightIdx = -1;

    function positionDropdown() {
        const rect = inp.getBoundingClientRect();
        dd.style.left = rect.left + 'px';
        dd.style.top = (rect.bottom) + 'px';
        dd.style.width = rect.width + 'px';
    }

    function renderItems(query) {
        const q = (query || '').toLowerCase();
        const filtered = items.filter(it => {
            if (!q) return true;
            return (it.label || '').toLowerCase().includes(q) ||
                   (it.code || '').toLowerCase().includes(q) ||
"@ -split "`r?`n"
    }
}

[System.IO.File]::WriteAllLines($file, $newLines, [System.Text.Encoding]::UTF8)
Write-Host "Injection complete."
