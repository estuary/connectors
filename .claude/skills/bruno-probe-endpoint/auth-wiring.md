# Bruno pre-request auth scripts

Reference for `bruno-probe-endpoint` Phase 3. The collection-level before-request script decrypts the connector's sops-encrypted `config.yaml` with Node's `child_process` and attaches the credential to `req`. One script serves both runtimes (`bru run … --sandbox developer`, and the desktop app in developer sandbox mode).

Contract for either style:

- `execFileSync('sops', ['-d', '--output-type=json', configPath], { encoding: 'utf8' })`, argv form. Let sops failures throw.
- The credential lives on `req` only (`API-TOKEN-EPHEMERAL`).
- The JSON path is the one Phase 2 derived from `models.py` for this connector; the paths below are placeholders.

## Style A — static token

The live reference is `source-mailchimp-native/bruno/opencollection.yml` (`request.scripts`, `type: before-request`). Copy from there, then change the JSON path and the header the provider expects (`Authorization: Bearer`, Basic, or a custom header, per the connector's `TokenSource`).

## Style B — OAuth refresh token

No sibling collection carries this style yet, so this block is the reference. Exchange the stored `refresh_token` for an access token on every request; the per-request roundtrip is what keeps the access token off disk.

```yaml
request:
  scripts:
    - type: before-request
      code: |-
        const { execFileSync } = require('child_process');
        const configPath = bru.getEnvVar('config_path');
        if (!configPath) throw new Error('config_path env var is not set');
        const decrypted = execFileSync(
          'sops', ['-d', '--output-type=json', configPath], { encoding: 'utf8' },
        );
        const creds = JSON.parse(decrypted)?.credentials;
        if (!creds?.client_id || !creds?.client_secret || !creds?.refresh_token) {
          throw new Error('credentials.{client_id,client_secret,refresh_token} missing');
        }

        const resp = await bru.sendRequest({
          method: 'POST',
          url: 'https://<provider>/oauth/token',
          headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
          data:
            'grant_type=refresh_token' +
            `&client_id=${encodeURIComponent(creds.client_id)}` +
            `&client_secret=${encodeURIComponent(creds.client_secret)}` +
            `&refresh_token=${encodeURIComponent(creds.refresh_token)}`,
        });
        const accessToken = resp?.data?.access_token;
        if (!accessToken) throw new Error(`token refresh failed: ${JSON.stringify(resp?.data ?? resp)}`);
        req.setHeader('Authorization', `Bearer ${accessToken}`);
```

Token endpoints are normally far above the connector's rate-limit budget. If this provider's is not, surface that as its own finding.
