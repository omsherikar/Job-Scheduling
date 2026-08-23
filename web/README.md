# fenceline dashboard

Next.js operator dashboard. Talks to the Go API on `API_URL`, default
`http://localhost:3001`.

```
npm install
npm run dev
```

HTTP calls go through the Next rewrite at `/api`, so they are same origin and the
bearer token is enough. The websocket connects straight to the API, which means two
things need setting when the dashboard is not served from the API's own origin:

- `NEXT_PUBLIC_API_URL` on the dashboard, so the browser knows where to connect.
- `API_ALLOWED_ORIGINS` on the API, a comma separated list of origins allowed to open
  a stream. Without it only same origin connections are accepted.

```
API_ALLOWED_ORIGINS=localhost:3010 DATABASE_URL=... go run ./cmd/api
```

Browsers cannot set an `Authorization` header on a websocket, so the session token
rides in as the `fl.token.<token>` subprotocol.
