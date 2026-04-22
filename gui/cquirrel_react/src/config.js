const BACKEND_HOST = window.location.hostname || '127.0.0.1';
const BACKEND_PORT = process.env.REACT_APP_BACKEND_PORT || '5051';

export const BACKEND_ORIGIN = `http://${BACKEND_HOST}:${BACKEND_PORT}`;
export const BACKEND_WS_ORIGIN = `${BACKEND_ORIGIN}/ws`;
