const parseCSVLine = (line) => {
    if (!line) return [];
    let delimiter = ',';
    if (line.includes(';') && (line.split(';').length > line.split(',').length)) {
        delimiter = ';';
    }
    const result = []; let current = ''; let inQuotes = false;
    for (let i = 0; i < line.length; i++) {
        const ch = line[i];
        if (inQuotes) {
            if (ch === '"') { if (i + 1 < line.length && line[i + 1] === '"') { current += '"'; i++; } else { inQuotes = false; } }
            else { current += ch; }
        } else {
            if (ch === '"') { inQuotes = true; }
            else if (ch === delimiter) { result.push(current); current = ''; }
            else { current += ch; }
        }
    }
    result.push(current);
    return result;
};

const test1 = 'C01,Name 1,Customer,123';
const test2 = 'C01;Name 1;Customer;123';
const test3 = 'C01,"Name, with comma",Customer,123';
const test4 = 'C01;"Name; with semi";Customer;123';

console.log('Test 1 (Comma):', JSON.stringify(parseCSVLine(test1)));
console.log('Test 2 (Semi):', JSON.stringify(parseCSVLine(test2)));
console.log('Test 3 (Quotes Comma):', JSON.stringify(parseCSVLine(test3)));
console.log('Test 4 (Quotes Semi):', JSON.stringify(parseCSVLine(test4)));
