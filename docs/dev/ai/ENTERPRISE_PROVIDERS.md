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

- **Provider**: Choose from OpenAI Compatible, Anthropic Claude, or Enterprise (Custom API)
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

1. Export your internal CA certificate (usually `.pem` or `.cer` format)
2. Convert to PKCS12 format if needed:
   ```bash
   keytool -import -alias company-ca \
     -file ca-certificate.pem \
     -keystore truststore.jks \
     -storepass changeit
   ```
3. In Prospector Settings:
   - Set **Truststore Path**: `/opt/drill/certs/truststore.jks`
   - Set **Truststore Password**: `changeit`
   - Set **Truststore Type**: `JKS`
   - Keep **Verify SSL Certificates** enabled

#### Client Certificate (mTLS)

If the AI API requires mutual TLS (client certificate authentication):

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

## Troubleshooting

### Connection Test Fails

**Problem**: "Connection test failed"

**Solutions:**
1. Verify API endpoint URL is correct and accessible
2. Check firewall rules allow connection
3. If proxy is configured, verify proxy URL and credentials
4. Check API key is valid (if required)
5. Check certificate configuration (truststore exists and password is correct)

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
