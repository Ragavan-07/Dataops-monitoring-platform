// Empty base URL = same origin. nginx (Docker) or the Vite proxy (dev) forwards /api to FastAPI.
const BASE_URL = import.meta.env.VITE_API_URL || "";

export async function getJSON(path) {
  const response = await fetch(`${BASE_URL}${path}`);
  if (!response.ok) {
    let detail = `${response.status} ${response.statusText}`;
    try {
      const body = await response.json();
      if (body && body.detail) detail = body.detail;
    } catch {
      // response body was not JSON; keep the status text
    }
    throw new Error(detail);
  }
  return response.json();
}
