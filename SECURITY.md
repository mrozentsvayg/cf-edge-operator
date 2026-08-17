# Security Policy

## Reporting a vulnerability

Please do not report security vulnerabilities through public GitHub issues.

Instead, use GitHub's private vulnerability reporting: open the repository's
**Security** tab and click **Report a vulnerability**
(https://github.com/mrozentsvayg/cf-edge-operator/security/advisories/new).

Please include:

- the affected version (release tag or commit)
- a description of the issue and its impact
- steps to reproduce, if available

This operator handles Cloudflare API tokens and manages edge resources (custom
hostnames, zones, TLS), so credential handling, RBAC scope, and origin/hostname
validation are all in scope.

## Response

We aim to acknowledge reports within a few business days, keep you updated on
remediation, and coordinate disclosure once a fix is available.

## Supported versions

Fixes are applied to `main` and the latest released version.
