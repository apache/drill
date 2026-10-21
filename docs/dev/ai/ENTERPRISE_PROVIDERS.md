<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# Enterprise AI Provider Configuration

This guide covers configuring Apache Drill's Prospector AI assistant to work with internal/enterprise AI models in corporate networks. It explains how to set up network connectivity, security certificates, authentication, and custom API formats.

## Quick Start

### For Internal OpenAI-Compatible Models

If you're running an internal instance of an OpenAI-compatible API (vLLM, TGI, Ollama, etc.):

1. Open **Prospector Settings** (gear icon in Prospector sidebar)
2. Set **LLM Provider** to "OpenAI Compatible"
3. Set **API Endpoint** to your internal endpoint: `http://internal-ai.company.com:8000/v1`
4. Set **Model** to your model name (e.g., `llama2-7b-chat`)
5. Leave **API Key** empty (if no auth required) or enter your internal API key
6. Click **Test Connection** to verify
7. Click **Save**

### For OpenAI-Compatible Models Behind an OAuth2 Gateway

If your model is OpenAI-compatible but fronted by an enterprise API gateway that requires
an OAuth2 client-credentials token (and often mTLS):

1. Set **LLM Provider** to "OAuth2 Gateway"
2. Set **API Endpoint** to the chat endpoint and **Model** to your model name
3. Fill in **Auth URL**, **Consumer Key**, **Consumer Secret** (the token is fetched automatically)
4. Fill in **Client ID**, **Usecase ID**, and **API Key** if your gateway's headers use them
5. Set **Client Certificate (PEM)** if the gateway requires mTLS
6. Click **Test Connection** — this performs the real token fetch *and* endpoint call
7. Click **Save**

See [OAuth2 Gateway Provider](#oauth2-gateway-provider) for the full flow and configurable headers.

### For Custom Enterprise APIs

If you have a proprietary API format:

1. Set **LLM Provider** to "Enterprise (Custom API)"
2. Set **API Endpoint** to your API URL
3. Define **Request Template** (JSON with variable substitution)
4. Define **Response Mapping** (JSONPath to extract response fields)
5. Click **Test Connection**
6. Click **Save**

## Configuration Options

### Basic Configuration

- **Provider**: Choose from OpenAI Compatible, Anthropic Claude, OAuth2 Gateway, or Enterprise (Custom API)
- **API Endpoint**: Full URL to your API server
- **API Key**: Authentication token (optional for local endpoints)
- **Model**: Model name or ID
- **Max Tokens**: Maximum response length (default: 4096)
- **Temperature**: Randomness/creativity (0-2, default: 0.7)
- **Enable Prospector**: Master toggle for all AI features

### Network Configuration

#### HTTP Proxy

If your network requires an HTTP/HTTPS proxy:

1. Set **HTTP Proxy URL**: `http://proxy.company.com:8080`
2. Set **Proxy Username** and **Proxy Password** (if authentication required)
3. Set **Timeouts** if network is slow:
   - Connect: 1-300 seconds (default: 30)
   - Read: 1-600 seconds (default: 120)
   - Write: 1-300 seconds (default: 30)

Example: Corporate proxy with authentication
```
HTTP Proxy URL: http://proxy.company.com:8080
Proxy Username: john.doe@company
Proxy Password: (enter in password field)
Connect Timeout: 45 seconds
```

### SSL/TLS Configuration

#### Trusting Internal CA

If your internal API uses HTTPS with a self-signed certificate or internal CA:

**Option A — PEM CA bundle (simplest).** Point **Truststore Path** directly at a PEM file
containing one or more `-----BEGIN CERTIFICATE-----` blocks (the same CA-bundle file a Python
`requests`/`httpx` client passes as `verify=`). The PEM format is auto-detected; no keystore
conversion or password is needed. This is the right place for a **certificates-only** file (no
private key) — that is a CA bundle for verifying the *server*, not a client certificate.

```
Truststore Path: /opt/drill/certs/ca-bundle.pem
Verify SSL Certificates: (keep enabled)
```

**Option B — JKS/PKCS12 truststore.** Convert the CA to a keystore if you prefer:
```bash
keytool -import -alias company-ca \
  -file ca-certificate.pem \
  -keystore truststore.jks \
  -storepass changeit
```
Then set **Truststore Path** / **Truststore Password** / **Truststore Type: JKS**, and keep
**Verify SSL Certificates** enabled.

#### Client Certificate (mTLS)

If the AI API requires mutual TLS (client certificate authentication). Note this is **not** the
same as the CA bundle above — a client certificate file must contain a **private key** as well as
the certificate. A certificates-only PEM belongs in the truststore, not here.

1. Obtain your client certificate and private key
2. Convert to PKCS12 format if needed:
   ```bash
   openssl pkcs12 -export \
     -in client-cert.pem \
     -inkey client-key.pem \
     -out client-keystore.p12 \
     -name client-cert
   ```
3. In Prospector Settings:
   - Set **Keystore Path**: `/opt/drill/certs/client-keystore.p12`
   - Set **Keystore Password**: (your keystore password)
   - Set **Keystore Type**: `PKCS12`

##### PEM client certificate (OAuth2 Gateway provider)

The **OAuth2 Gateway** provider also accepts a single PEM file directly (the same
`cert=` form used by Python `requests`/`httpx`) via its **Client Certificate (PEM)** field —
no keystore conversion needed. The file must contain the client certificate chain plus an
**unencrypted PKCS#8** private key (`-----BEGIN PRIVATE KEY-----`).

If your key is PKCS#1 (`BEGIN RSA PRIVATE KEY`) or SEC1 (`BEGIN EC PRIVATE KEY`), convert it:
```bash
openssl pkcs8 -topk8 -nocrypt -in client.pem -out client-pkcs8.pem
```
The certificate is presented on **both** the token call and the endpoint call.

#### Self-Signed Certificates (Development Only)

For testing with self-signed certificates:

1. Uncheck **Verify SSL Certificates** (⚠️ WARNING: insecure, development only)
2. Log output will show: "SSL certificate verification is disabled"

### Authentication

#### Custom Headers

Add custom authentication headers that your API requires:

```json
{
  "X-API-Key": "your-api-key",
  "X-Tenant-ID": "tenant-123",
  "Authorization": "Bearer your-token"
}
```

Each request will include these headers automatically.

### Static Request Parameters

Send additional parameters with every API request:

```json
{
  "environment": "production",
  "region": "us-east-1",
  "version": "v2"
}
```

These are merged into the request body.

## Custom API Format

The **Enterprise (Custom API)** provider supports custom request/response formats for proprietary APIs.

### Request Template

Define how to format requests using variable substitution:

```json
{
  "input": "{messages}",
  "model_config": {
    "name": "{model}",
    "temperature": {temperature},
    "max_output_tokens": {maxTokens}
  },
  "additional_params": {
    "version": "2024-01-01"
  }
}
```

**Supported variables:**
- `{model}` - Model name from configuration
- `{temperature}` - Temperature setting
- `{maxTokens}` - Max tokens setting
- `{messages}` - Array of chat messages in OpenAI format
- `{tools}` - Array of available tools

### Response Mapping

Define how to extract response data from custom API responses using JSONPath:

```json
{
  "contentPath": "$.response.text",
  "donePath": "$.metadata.finish_reason",
  "toolCallPath": "$.response.function_call"
}
```

**Supported paths:**
- `contentPath` - Path to text content delta
- `donePath` - Path to finish reason ("stop" or "tool_calls")
- `toolCallPath` - Path to tool call data

If no mapping is provided, the response is assumed to be in normalized format (with "type", "content", "finish_reason" fields).

### Example: Custom API Integration

#### Request Template for Custom Model
```json
{
  "prompt": "{messages}",
  "model": "{model}",
  "parameters": {
    "temperature": {temperature},
    "max_length": {maxTokens},
    "top_p": 0.95
  }
}
```

#### Response Mapping
```json
{
  "contentPath": "$.output.text",
  "donePath": "$.output.status"
}
```

## OAuth2 Gateway Provider

The **OAuth2 Gateway** provider targets an OpenAI-compatible chat API that sits behind an
enterprise API gateway (Apigee and similar) using **OAuth2 client-credentials** authentication
and, usually, **mTLS**. Because the chat API itself is OpenAI-compatible, this provider reuses
the OpenAI request/response handling and only adds the gateway authentication.

### How it works

On every chat request (and on **Test Connection**), the provider:

1. **Fetches a bearer token** from the **Auth URL** using OAuth2 client-credentials:
   ```
   POST {authUrl}
   Authorization: Basic base64(consumerKey:consumerSecret)
   Content-Type: application/x-www-form-urlencoded

   grant_type=client_credentials
   ```
   It reads `access_token` from the JSON response and caches it until ~60s before `expires_in`
   (numeric or numeric-string). The token is re-fetched automatically when it expires.
2. **Calls the endpoint** with the OpenAI-compatible body plus the configured gateway headers
   (see below), including the fetched token.

mTLS (see [PEM client certificate](#pem-client-certificate-oauth2-gateway-provider)) is
presented on both calls.

### Configuration fields

| Field | Purpose |
|---|---|
| **API Endpoint** | Chat endpoint (OpenAI-compatible). `/chat/completions` is appended if absent. |
| **Model** | Model name. |
| **Auth URL** | OAuth2 token endpoint. |
| **Consumer Key** / **Consumer Secret** | Basic-auth credentials for the token call. |
| **Client ID** / **Usecase ID** | Referenced by the default gateway headers; optional otherwise. |
| **API Key** | Referenced by the default gateway headers; optional otherwise. |
| **Client Certificate (PEM)** | Path to the mTLS client cert + PKCS#8 key. |
| **Gateway Headers (JSON)** | Header name → value template (see below). Blank = default mapping. |

Only **API Endpoint**, **Model**, **Auth URL**, **Consumer Key**, and **Consumer Secret** are
required; the rest depend on what your gateway headers reference.

### Configurable gateway headers

Header **names are fully configurable** so the provider works with any gateway. The **Gateway
Headers (JSON)** field maps a header name to a value template. Templates may contain these
placeholders (anything else is sent literally):

| Placeholder | Resolves to |
|---|---|
| `{token}` | the fetched OAuth2 bearer token |
| `{uuid}` | a fresh UUID, generated per request |
| `{timestamp}` | local ISO-8601 timestamp with microseconds, e.g. `2026-07-10T09:25:58.123456` |
| `{apiKey}` | the **API Key** field |
| `{clientId}` | the **Client ID** field |
| `{usecaseId}` | the **Usecase ID** field |
| `{model}` | the **Model** field |

Leaving the field blank applies the default mapping:

```json
{
  "Authorization": "Bearer {token}",
  "client-id": "{clientId}",
  "usecase-id": "{usecaseId}",
  "api-key": "{apiKey}",
  "x-request-id": "{uuid}",
  "x-correlation-id": "{uuid}",
  "request-date": "{timestamp}"
}
```

For a gateway with different header names or a non-Bearer scheme, override it, e.g.:

```json
{
  "X-Api-Token": "{token}",
  "X-Request-Trace": "{uuid}",
  "X-Model": "{model}"
}
```

> **Timestamp note:** `{timestamp}` uses **local** time (matching a Python `datetime.now().isoformat()`
> reference client). If your gateway validates the date as UTC and rejects requests as stale, that is
> the first thing to adjust.

## Configuration Examples

### Example 1: Local Ollama (No Auth)

```
Provider: OpenAI Compatible
API Endpoint: http://localhost:11434/v1
API Key: (empty)
Model: llama2
```

### Example 2: Internal vLLM with Proxy

```
Provider: OpenAI Compatible
API Endpoint: http://vllm-internal.company.com:8000/v1
API Key: sk-internal-key-12345
HTTP Proxy URL: http://proxy.company.com:8080
Proxy Username: john@company
Proxy Password: (from password manager)
Connect Timeout: 45s
```

### Example 3: mTLS Protected API

```
Provider: Enterprise (Custom API)
API Endpoint: https://ai-api.company.com/v1
HTTP Proxy URL: http://proxy.company.com:8080
Truststore Path: /opt/drill/certs/ca-truststore.jks
Truststore Password: truststore-pass
Keystore Path: /opt/drill/certs/client-keystore.jks
Keystore Password: keystore-pass
Custom Headers: {"X-Api-Key": "internal-key", "X-Client-Id": "drill-001"}
```

### Example 4: Custom API with Request Template

```
Provider: Enterprise (Custom API)
API Endpoint: https://internal-llm.company.com/chat
Request Template:
{
  "query": "{messages}",
  "config": {
    "model": "{model}",
    "temperature": {temperature}
  }
}
Response Mapping:
{
  "contentPath": "$.data.response",
  "donePath": "$.metadata.complete"
}
```

### Example 5: OpenAI-Compatible Model Behind an OAuth2 Gateway (mTLS)

```
Provider: OAuth2 Gateway
API Endpoint: https://gateway.company.com/ai/v1
Model: gpt-4o
Auth URL: https://gateway.company.com/oauth2/v1/token
Consumer Key: (enter in field)
Consumer Secret: (enter in password field)
Client ID: drill-prod
Usecase ID: analytics-001
API Key: (enter in password field)
Client Certificate (PEM): /opt/drill/certs/gateway-client.pem
Gateway Headers: (blank — uses the default mapping)
```

Leaving **Gateway Headers** blank uses the default mapping (`client-id`, `usecase-id`,
`api-key`, `x-request-id`, `x-correlation-id`, `request-date`). Set it explicitly whenever
your gateway needs different header names or a non-Bearer scheme — see
[Configurable gateway headers](#configurable-gateway-headers).

## Troubleshooting

### Connection Test Fails

**Problem**: "Connection test failed"

**Read the details first.** When a test fails, the result banner has an expandable **Details**
panel with the exact URL called, the HTTP status and full response body, or — for connection
failures — the **full exception cause chain** (e.g. an `SSLHandshakeException` caused by
`PKIX path building failed`, the real signal for a missing CA). The same detail is written to the
Drillbit log. `Test Connection` exercises the real request path, so for the OAuth2 Gateway
provider it tells you which leg broke — token fetch vs. mTLS vs. the endpoint call.

**Solutions:**
1. Verify API endpoint URL is correct and accessible
2. Check firewall rules allow connection
3. If proxy is configured, verify proxy URL and credentials
4. Check API key is valid (if required)
5. Check certificate configuration (truststore exists and password is correct)
6. For the OAuth2 Gateway provider: if the Details show a non-2xx from the **Auth URL**, the
   Consumer Key/Secret or Auth URL is wrong; if the token succeeds but the endpoint returns 401,
   check the gateway headers; a `PKIX`/`unable to find valid certification path` error means the
   gateway's CA is missing from your truststore, or the client PEM is wrong for mTLS.

### SSL Certificate Errors

**Problem**: "PKIX path building failed" or "self-signed certificate"

**Solutions:**
1. Verify truststore path is correct: `ls -la /opt/drill/certs/truststore.jks`
2. Verify truststore password is correct
3. Check certificate is in truststore:
   ```bash
   keytool -list -v -keystore truststore.jks
   ```
4. For self-signed certs (development only), uncheck "Verify SSL Certificates"

### Proxy Authentication Fails

**Problem**: "407 Proxy Authentication Required"

**Solutions:**
1. Verify proxy username and password
2. Check proxy is reachable: `curl -v -x http://proxy.company.com:8080 http://google.com`
3. Try without proxy first to isolate the issue
4. Check if proxy requires special authentication format

### Custom API Response Not Parsed

**Problem**: Response mapping produces empty results

**Solutions:**
1. Test API manually to see actual response format:
   ```bash
   curl -X POST https://api.company.com/v1 \
     -H "Content-Type: application/json" \
     -d '{"query": "test"}' | jq .
   ```
2. Update response mapping JSONPath to match actual response
3. Check response is valid JSON using JSONPath tool online
4. Review Drill logs for parsing errors

## Security Best Practices

### 1. Credential Management

- Store passwords securely (use Drill's encrypted configuration)
- Don't commit credentials to version control
- Use different credentials per environment (dev/staging/prod)
- Rotate credentials regularly

### 2. Network Security

- Use HTTPS for all production APIs
- Enable SSL certificate verification (only disable for development)
- Use internal proxies for traffic inspection/logging
- Restrict firewall rules to necessary services only

### 3. Certificate Management

- Keep truststore updated with internal CA certificates
- Backup keystores and truststores
- Document certificate expiration dates
- Set up alerts for certificate expiration

### 4. API Keys

- Use least-privilege API keys
- Rotate keys regularly
- Monitor key usage for anomalies
- Store keys in secure credential stores (not plaintext configs)

### 5. Custom Headers

- Don't include sensitive data in custom headers if traffic is logged
- Use standard authentication headers (Authorization, X-API-Key)
- Document which headers are required

## Maintenance

### Updating Configuration

Configuration changes are applied immediately after clicking Save. No restart required.

### Testing Changes

Use "Test Connection" button to verify:
- Network connectivity
- Proxy configuration
- Certificate validation
- API credentials
- Custom request format (for Enterprise provider)

### Monitoring

Enable debug logging to see HTTP requests/responses:
```
# In log4j2.xml
<Logger name="org.apache.drill.exec.server.rest.ai" level="debug"/>
```

Debug logs will show:
- Outgoing HTTP requests (with headers redacted)
- Response status codes
- Any parsing errors

## FAQ

**Q: Can I use multiple AI providers?**
A: No, only one provider is configured at a time. To switch, update the Provider setting and save.

**Q: Do I need to restart Drill after configuration changes?**
A: No, changes take effect immediately.

**Q: What happens if the AI API is unavailable?**
A: Connection test will fail. Prospector will show an error message. Check network connectivity and API status.

**Q: Can I use custom headers for authentication instead of API Key?**
A: Yes. Leave API Key empty and set custom headers with your auth token (e.g., `Authorization: Bearer ...`).

**Q: How do I use internal AI models without exposing them to the internet?**
A: Configure proxy to route through your corporate network, use mTLS for authentication, and place the AI API behind a firewall accessible only from Drill.

**Q: What certificate format should my truststore be?**
A: JKS or PKCS12. JKS is the Java default. Use keytool to import certificates into truststore.

## See Also

- [AI_FEATURES.md](../AI_FEATURES.md) - Overview of all AI features
- [PROSPECTOR.md](../PROSPECTOR.md) - Prospector AI assistant reference
- [LicenseHeadersAndNotices.md](../LicenseHeadersAndNotices.md) - License requirements
