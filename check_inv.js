const fs = require('fs');
const appjs = fs.readFileSync('app.js', 'utf8');
const urlMatch = appjs.match(/SUPABASE_URL = '([^']+)'/);
const keyMatch = appjs.match(/SUPABASE_KEY = '([^']+)'/);
if(urlMatch && keyMatch) {
  fetch(`${urlMatch[1]}/rest/v1/inventory?select=*&limit=1`, {
    headers: { 'apikey': keyMatch[1], 'Authorization': `Bearer ${keyMatch[1]}` }
  }).then(r=>r.json()).then(data => {
      console.log(data);
  }).catch(console.error);
}
