const { createClient } = require('@supabase/supabase-js');

const SUPABASE_URL = 'https://pfukfcnxvrkefcmevcxq.supabase.co';
const SUPABASE_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InBmdWtmY254dnJrZWZjbWV2Y3hxIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzM0NTk2MjksImV4cCI6MjA4OTAzNTYyOX0.tPCMJ431g5iHb9qkRSzMWlV0dL_iVPNXPnQjJ0DwZPw';

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

async function test() {
    const { data, error } = await supabase.from('users').select('*');
    if (error) {
        console.error('Error:', error);
    } else {
        console.log('Users count:', data.length);
        console.log('Users:', data);
    }
}

test();
