# Quickstart: a cluster and its console

One command starts a small clink cluster from the published runtime image and
submits two jobs to it, so the ops console has something to show:

```bash
curl -fsSLO https://raw.githubusercontent.com/orhaugh/clink/main/examples/quickstart/compose.yml
docker compose -f compose.yml up
```

Then open <http://localhost:8081>. The Coordinator serves the console on the
same port as its JSON API; nothing else is installed.

| Service | What it runs |
|---------|--------------|
| `coordinator` | the Coordinator, its HTTP API and the console on `localhost:8081` |
| `worker1`, `worker2` | two Workers with eight slots each |
| `submit` | a one-shot that submits `demo-stream`, a job that ticks every 100 ms until you stop it, and `readings-by-region`, a SQL job over a file the image carries, which finishes |

In the console, `demo-stream` reads as streaming and `readings-by-region` as
completed. The job pages show each job's topology, its delivery guarantee and
how it checkpoints; the Workers page shows where every subtask runs.

`docker compose -f compose.yml down` stops and removes everything.
`CLINK_IMAGE=<image>` runs another image, for example one you built.

The cluster takes no token, and everything listens on this machine's
loopback address only, so it is not for a shared network. For one, set
`CLINK_AUTH_TOKEN` on every node: the API then answers only requests that
carry it, and the console asks for it once and sends it on every call.
