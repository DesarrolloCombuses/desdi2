const { createClient } = window.supabase;

const sb = createClient(
  'https://cbplebkmxrkaafqdhiyi.supabase.co',
  'sb_publishable_DZCceNTENY4ViP17-eZrGg_bdMElZ9X'
);

const loginForm = document.getElementById('loginForm');
const loginEmail = document.getElementById('loginEmail');
const loginPassword = document.getElementById('loginPassword');
const loginMessage = document.getElementById('loginMessage');

if (!loginForm || !loginEmail || !loginPassword || !loginMessage) {
  throw new Error('Faltan elementos del formulario de login.');
}

function setLoginMessage(type, text) {
  loginMessage.className = `message ${type}`;
  loginMessage.textContent = text;
}

async function ensureProfile(user) {
  if (!user) return;

  const meta = user.user_metadata || {};
  const username = meta.username || user.email?.split('@')[0] || `user_${user.id.slice(0, 8)}`;
  const fullName = meta.full_name || '';

  const { error } = await sb.from('profiles').upsert(
    {
      id: user.id,
      username,
      full_name: fullName
    },
    { onConflict: 'id' }
  );

  if (error) throw error;
}

loginForm.addEventListener('submit', async (e) => {
  e.preventDefault();

  try {
    setLoginMessage('success', 'Validando acceso...');

    const { data, error } = await sb.auth.signInWithPassword({
      email: loginEmail.value.trim(),
      password: loginPassword.value
    });

    if (error) throw error;
    if (!data.user) throw new Error('No se pudo iniciar sesion.');

    try {
      await ensureProfile(data.user);
    } catch (profileErr) {
      console.warn('No se pudo crear/actualizar perfil:', profileErr.message);
    }

    setLoginMessage('success', 'Acceso autorizado. Redirigiendo...');
    setTimeout(() => { location.href = 'dashboard.html'; }, 700);
  } catch (err) {
    setLoginMessage('error', err.message);
  }
});

(async () => {
  try {
    const { data: { user }, error } = await sb.auth.getUser();
    if (error) throw error;
    if (!user) return;
    location.href = 'dashboard.html';
  } catch (err) {
    setLoginMessage('error', err.message);
  }
})();






