# Agentic Agency Hermes plugin

`agentic_agency` is the Hermes Agent plugin of the Agentic Agency Chat iOS app. It runs inside
Hermes next to the Agentic Agency BFF (binaries: https://github.com/FlorianCP/Agentic-Agency-Chat-App-BFF)
and forwards two things the Hermes API no longer reports for app runs since Hermes v0.22.3:

- the served provider and model of each main-loop model call (`post_api_request` hook), so the
  app can show which model answered, and
- live reasoning deltas (`on_stream_delta` hook, opt-in), so the app can show the assistant
  thinking while it works.

Events go to the BFF on localhost (`POST /v1/plugin/events`) with a dedicated secret that is
separate from the app's pairing token. Delivery is a bounded background queue: a hook never
blocks or fails a turn, and nothing is logged with message content or the secret. Python 3.11+
standard library only; no third-party dependencies. Works with Hermes v0.22.2 and v0.22.5.

This repository contains release builds only. Every commit is one release, tagged
`v<version>`; the head of `main` is the latest release. The source and the authoritative
runbooks live in the app repository (`hermes-plugin/`, `bff/PLUGIN.md`, `bff/SETUP.md`).

## Install (per Hermes profile)

Install pinned to the full release commit, never a moving branch:

```sh
export HERMES_HOME=/path/to/the/profile/home
hermes plugins install FlorianCP/Agentic-Agency-Chat-App-Hermes-Plugin --ref <full-40-char-commit> --no-enable
hermes plugins enable agentic_agency
```

Then create `$HERMES_HOME/agentic_agency/plugin.json` with mode 0600 (directory 0700):

```json
{"bff_url":"http://127.0.0.1:8643","secret":"<the plugin secret also set as plugin.secret in the BFF config>"}
```

and merge into that profile's `config.yaml`:

```yaml
plugins:
  enabled:
    - agentic_agency
  stream_reasoning_deltas: true
```

Restart the Hermes gateway to load the plugin, then confirm in the BFF capabilities that the
plugin reports `connected: true` with the expected version. A missing or wrongly permissioned
config file leaves the plugin inert with a single warning.

## Update and removal

- Update: `hermes plugins install FlorianCP/Agentic-Agency-Chat-App-Hermes-Plugin --force --ref <new-full-commit>`,
  then restart the gateway.
- Disable: `hermes plugins disable agentic_agency`. Remove: `hermes plugins remove agentic_agency`,
  then delete `$HERMES_HOME/agentic_agency/plugin.json` and remove `plugin.secret` from the BFF config.
