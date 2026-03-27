const _clean = (obj) => {
    if (!obj || typeof obj !== 'object' || Array.isArray(obj)) return obj;
    const res = {};
    for (const key in obj) {
        const v = obj[key];
        res[key] = (v === '' && (key.includes('date') || key.includes('at') || key.includes('_at') || key === 'item_code' || key === 'party_code' || key === 'group')) ? null : v;
    }
    return res;
};

const test1 = { name: 'Test', party_code: '', phone: '' };
const test2 = { name: 'Test', item_code: '', price: 10 };
const test3 = { date: '', notes: 'bla' };

console.log('Test 1 (Party Code):', JSON.stringify(_clean(test1)));
console.log('Test 2 (Item Code):', JSON.stringify(_clean(test2)));
console.log('Test 3 (Date):', JSON.stringify(_clean(test3)));
