export const API = "/api";

export async function fetchJSON(url) {
  const res = await fetch(url);
  if (!res.ok) {
    throw new Error(`Request failed: ${res.status}`);
  }
  return res.json();
}

export async function fetchJSONWithInit(url, init) {
  const res = await fetch(url, init);
  if (!res.ok) {
    let detail = "";
    try {
      const body = await res.json();
      detail = body?.detail ? `: ${body.detail}` : "";
    } catch {
      // ignore parse errors
    }
    throw new Error(`Request failed: ${res.status}${detail}`);
  }
  return res.json();
}
