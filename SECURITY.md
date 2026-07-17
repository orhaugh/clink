# Security policy

## Supported versions

clink is pre-1.0. Only the latest commit on `main` is supported; fixes are
not backported to earlier states.

## Reporting a vulnerability

Please do not open a public issue for a suspected vulnerability. Use
GitHub's private vulnerability reporting on this repository (Security tab,
"Report a vulnerability"), or email orhaugh@gmail.com with the details and
a reproduction if you have one. You will get an acknowledgement within a
few days.

## Deployment model and expectations

The cluster control plane (JobManager/TaskManager RPC) and the HTTP API
are designed to run inside a trusted network. Operating beyond one:

- Enable TLS / mTLS on the transport (`clink_node --tls-cert` /
  `--tls-ca`, and `--tls-client-*` for mutual auth).
- Set `CLINK_AUTH_TOKEN` on `clink_node` so the HTTP control plane
  requires a bearer token; without it the API is open to whoever can reach
  the port. The HTTP server is off entirely unless `--http-port` is set.
- Treat job submission as code execution: a submitted job plugin (`.so`)
  runs native code on every TaskManager. Never expose submission endpoints
  to untrusted clients, in any configuration.
- WASM UDFs run sandboxed (self-contained modules, a fuel budget,
  bounds-checked guest memory), but the SQL surface as a whole is not
  designed to be exposed directly to untrusted users.
- Connector credentials can be supplied via `env://` indirection so
  secrets stay out of SQL text and job specs.
