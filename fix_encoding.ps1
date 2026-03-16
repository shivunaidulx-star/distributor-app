$file = "C:\Users\Admin\.gemini\antigravity\scratch\distributor-app\app.js"
$text = [IO.File]::ReadAllText($file)
# The text was read as Default (Win-1252) but contained UTF-8 bytes, so they got converted into literal characters.
# Let's get the bytes back using Windows-1252
$bytes = [Text.Encoding]::GetEncoding(1252).GetBytes($text)
# Now parse those bytes as UTF-8
$fixedText = [Text.Encoding]::UTF8.GetString($bytes)

# Now fix the sub-category bug in the fixed text
# Change 'onchange="onSOCatFilterChange()"' for the subcategory to 'onchange="onSOSubcatFilterChange()"'
$fixedText = $fixedText.Replace(
    '<select id="f-so-subcat-filter" onchange="onSOCatFilterChange()">',
    '<select id="f-so-subcat-filter" onchange="onSOSubcatFilterChange()">'
)

# And add the onSOSubcatFilterChange function below onSOCatFilterChange
$subcatFunc = @"
}

// Sub-Category filter handler
function onSOSubcatFilterChange() {
    var cat = $('f-so-cat-filter').value;
    var sc = $('f-so-subcat-filter').value;
    
    var inv = DB.get('db_inventory') || [];
    if (cat) inv = inv.filter(function(i) { return (i.category || '') === cat; });
    if (sc) inv = inv.filter(function(i) { return (i.subCategory || '') === sc; });
    
    $('f-so-item-input').value = '';
    $('f-so-price').value = '';
    _soItemDropdown = initSearchDropdown('f-so-item-input', buildItemSearchList(inv), function(item) {
        $('f-so-price').value = item.salePrice || '';
        var uomSel = $('f-so-uom');
        if (uomSel) {
            uomSel.innerHTML = '<option value="' + item.unit + '">' + item.unit + '</option>';
            if (item.secUom) uomSel.innerHTML += '<option value="' + item.secUom + '">' + item.secUom + '</option>';
        }
    });
}
"@

$fixedText = $fixedText.Replace("    });`r`n}", "    });`r`n" + $subcatFunc)

[IO.File]::WriteAllText($file, $fixedText, [Text.Encoding]::UTF8)
Write-Host "Encoding fixed and sub-category handler added."
