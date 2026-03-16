$file = "C:\Users\Admin\.gemini\antigravity\scratch\distributor-app\app.js"
$lines = [System.IO.File]::ReadAllLines($file)

$output = New-Object System.Collections.Generic.List[string]
$skip = $false
$funcFound = $false

for ($i = 0; $i -lt $lines.Count; $i++) {
    $line = $lines[$i]
    $trimmed = $line.Trim()
    
    # Replace customer datalist with plain input
    if ($trimmed -match 'id="f-so-party" list="dl-customers"') {
        $output.Add($line.Replace(' list="dl-customers"', '').Replace('Type name or mobile...', 'Type customer name or mobile...'))
        continue
    }
    
    # Skip datalist dl-customers blocks
    if ($trimmed -eq '<datalist id="dl-customers">') {
        $skip = $true
        continue
    }
    if ($skip -and $trimmed -eq '</datalist>') {
        $skip = $false
        continue
    }
    if ($skip) { continue }
    
    # Replace item input - remove list and onchange attributes
    if ($trimmed -match 'id="f-so-item-input" list="dl-so-items"') {
        $newLine = $line -replace ' list="dl-so-items"', '' -replace ' onchange="onSOItemChange\(\)"', ''
        $output.Add($newLine)
        continue
    }
    
    # Skip datalist dl-so-items blocks  
    if ($trimmed -match '<datalist id="dl-so-items">') {
        $skip = $true
        continue
    }
    if ($skip -and $trimmed -match '</datalist>') {
        $skip = $false
        continue
    }
    
    # Replace category filter onchange for SO 
    if ($trimmed -match "onSOCatChange\('f-so-cat-filter'") {
        $output.Add($line -replace "onSOCatChange\('f-so-cat-filter', 'f-so-subcat-filter', 'f-so-item-input', 'f-so-price'\)", "onSOCatFilterChange()")
        continue
    }
    if ($trimmed -match "filterSOItems\('f-so-cat-filter'") {
        $output.Add($line -replace "filterSOItems\('f-so-cat-filter', 'f-so-subcat-filter', 'f-so-item-input', 'f-so-price'\)", "onSOCatFilterChange()")
        continue
    }
    
    # After the openSalesOrderModal closing, insert init code
    # Detect: the line after `Submit Order</button></div>` template close line
    if ($trimmed -eq '`);' -and $funcFound) {
        $output.Add($line)
        # Add init code
        $output.Add('')
        $output.Add('    // Init custom searchable dropdowns (replaces broken native datalist)')
        $output.Add('    initSearchDropdown(''f-so-party'', buildPartySearchList(parties));')
        $output.Add('')
        $output.Add('    _soItemDropdown = initSearchDropdown(''f-so-item-input'', buildItemSearchList(inv), function(item) {')
        $output.Add('        $(''f-so-price'').value = item.salePrice || '''';')
        $output.Add('        var uomSel = $(''f-so-uom'');')
        $output.Add('        if (uomSel) {')
        $output.Add('            uomSel.innerHTML = ''<option value="'' + item.unit + ''">'' + item.unit + ''</option>'';')
        $output.Add('            if (item.secUom) uomSel.innerHTML += ''<option value="'' + item.secUom + ''">'' + item.secUom + ''</option>'';')
        $output.Add('        }')
        $output.Add('    });')
        
        # Skip the old closing brace
        if ($lines[$i+1].Trim() -eq '}') {
            $i++
        }
        
        $output.Add('}')
        $output.Add('var _soItemDropdown = null;')
        $output.Add('')
        $output.Add('// Category filter handler for SO modal')
        $output.Add('function onSOCatFilterChange() {')
        $output.Add('    var cat = $(''f-so-cat-filter'').value;')
        $output.Add('    var subCatSelect = $(''f-so-subcat-filter'');')
        $output.Add('    subCatSelect.innerHTML = ''<option value="">All Sub-Categories</option>'';')
        $output.Add('    if (cat) {')
        $output.Add('        var catObj = (DB.get(''db_categories'') || []).find(function(c) { return c.name === cat; });')
        $output.Add('        if (catObj && catObj.subCategories) {')
        $output.Add('            catObj.subCategories.forEach(function(sub) {')
        $output.Add('                subCatSelect.innerHTML += ''<option value="'' + sub + ''">'' + sub + ''</option>'';')
        $output.Add('            });')
        $output.Add('        }')
        $output.Add('    }')
        $output.Add('    var inv = DB.get(''db_inventory'') || [];')
        $output.Add('    if (cat) inv = inv.filter(function(i) { return (i.category || '''') === cat; });')
        $output.Add('    var sc = $(''f-so-subcat-filter'').value;')
        $output.Add('    if (sc) inv = inv.filter(function(i) { return (i.subCategory || '''') === sc; });')
        $output.Add('    $(''f-so-item-input'').value = '''';')
        $output.Add('    $(''f-so-price'').value = '''';')
        $output.Add('    _soItemDropdown = initSearchDropdown(''f-so-item-input'', buildItemSearchList(inv), function(item) {')
        $output.Add('        $(''f-so-price'').value = item.salePrice || '''';')
        $output.Add('        var uomSel = $(''f-so-uom'');')
        $output.Add('        if (uomSel) {')
        $output.Add('            uomSel.innerHTML = ''<option value="'' + item.unit + ''">'' + item.unit + ''</option>'';')
        $output.Add('            if (item.secUom) uomSel.innerHTML += ''<option value="'' + item.secUom + ''">'' + item.secUom + ''</option>'';')
        $output.Add('        }')
        $output.Add('    });')
        $output.Add('}')
        
        $funcFound = $false
        continue
    }
    
    # Mark when we enter openSalesOrderModal
    if ($trimmed -eq 'function openSalesOrderModal() {') {
        $funcFound = $true
    }
    
    $output.Add($line)
}

[System.IO.File]::WriteAllLines($file, $output.ToArray())
Write-Host "Done! Modified $($output.Count) lines"
