// Inicializar Supabase correctamente
const { createClient } = window.supabase;

const sb = createClient(
  'https://cbplebkmxrkaafqdhiyi.supabase.co',
  'sb_publishable_DZCceNTENY4ViP17-eZrGg_bdMElZ9X'
);


const tabLogin = document.getElementById("tabLogin");
const tabRegister = document.getElementById("tabRegister");
const loginForm = document.getElementById("loginForm");
const registerForm = document.getElementById("registerForm");
const loginEmail = document.getElementById("loginEmail");
const loginPassword = document.getElementById("loginPassword");
const loginMessage = document.getElementById("loginMessage");
const regEmail = document.getElementById("regEmail");
const regPassword = document.getElementById("regPassword");
const regUsername = document.getElementById("regUsername");
const regFullName = document.getElementById("regFullName");
const registerMessage = document.getElementById("registerMessage");

if (
  !tabLogin || !tabRegister || !loginForm || !registerForm ||
  !loginEmail || !loginPassword || !loginMessage ||
  !regEmail || !regPassword || !regUsername || !regFullName || !registerMessage
) {
  throw new Error("Faltan elementos en el HTML. Revisa los IDs del formulario.");
}

// Cambiar pestaÃ±as sin usar onclick inline
document.getElementById("tabLogin").addEventListener("click", () => {
  switchTab("login");
});

document.getElementById("tabRegister").addEventListener("click", () => {
  switchTab("register");
});

function switchTab(tab) {
  document.querySelectorAll(".auth-form").forEach(f => f.classList.remove("active"));
  document.querySelectorAll(".tab-btn").forEach(b => b.classList.remove("active"));

  if (tab === "login") {
    loginForm.classList.add("active");
    tabLogin.classList.add("active");
  } else {
    registerForm.classList.add("active");
    tabRegister.classList.add("active");
  }
}

async function ensureProfile(user) {
  if (!user) return;

  const meta = user.user_metadata || {};
  const username = meta.username || user.email?.split("@")[0] || `user_${user.id.slice(0, 8)}`;
  const fullName = meta.full_name || "";

  const { error } = await sb.from("profiles").upsert(
    {
      id: user.id,
      username,
      full_name: fullName
    },
    { onConflict: "id" }
  );

  if (error) throw error;
}

// REGISTRO
registerForm.addEventListener("submit", async (e) => {
  e.preventDefault();

  const msg = registerMessage;

  try {
    const { data, error } = await sb.auth.signUp({
      email: regEmail.value,
      password: regPassword.value,
      options: {
        data: {
          username: regUsername.value,
          full_name: regFullName.value
        }
      }
    });

    if (error) throw error;
    if (!data.user) throw new Error("No se pudo crear el usuario.");

    // Si hay sesión inmediata, creamos perfil ahora. Si no, se crea al iniciar sesión.
    if (data.session?.user) {
      await ensureProfile(data.session.user);
    }

    msg.className = "message success";
    msg.textContent = data.session
      ? "Registro exitoso."
      : "Cuenta creada. Revisa tu email y luego inicia sesión.";
    registerForm.reset();

  } catch (err) {
    msg.className = "message error";
    msg.textContent = err.message;
  }
});

// LOGIN
loginForm.addEventListener("submit", async (e) => {
  e.preventDefault();

  const msg = loginMessage;

  try {
    const { data, error } = await sb.auth.signInWithPassword({
      email: loginEmail.value,
      password: loginPassword.value
    });

    if (error) throw error;
    if (!data.user) throw new Error("No se pudo iniciar sesión.");

    // Intentamos asegurar el perfil del usuario sin bloquear el login si falla.
    try {
      await ensureProfile(data.user);
    } catch (profileErr) {
      console.warn("No se pudo crear/actualizar el perfil:", profileErr.message);
    }

    msg.className = "message success";
    msg.textContent = "Bienvenido...";
    setTimeout(() => location.href = "dashboard.html", 800);

  } catch (err) {
    msg.className = "message error";
    msg.textContent = err.message;
  }
});

// Si ya hay sesiÃ³n activa
(async () => {
  try {
    const { data: { user }, error } = await sb.auth.getUser();
    if (error) throw error;
    if (user) location.href = "dashboard.html";
  } catch (err) {
    loginMessage.className = "message error";
    loginMessage.textContent = err.message;
  }
})();

