# Security Policy

## Reporting Security Vulnerabilities

**Please do not report security vulnerabilities through public GitHub issues.**

If you discover a security vulnerability in Spring for Apache Kafka, please report it to the VMware Tanzu security team at [security@vmware.com](mailto:security@vmware.com).

Please include the following information in your report:

- Description of the vulnerability
- Steps to reproduce the issue
- Affected versions
- Potential impact
- Suggested fix (if available)

## Supported Versions

We provide security updates for the following versions:

| Version | Supported          |
| ------- | ------------------ |
| 3.3.x   | :white_check_mark: |
| 3.2.x   | :white_check_mark: |
| < 3.2   | :x:                |

## Security Scanning

This project uses multiple automated security scanning tools:

### Static Application Security Testing (SAST)
- **CodeQL**: Analyzes code for security vulnerabilities and coding errors
- Runs on: Push to main, Pull Requests, Daily schedule

### Dependency Vulnerability Scanning
- **OWASP Dependency Check**: Scans project dependencies for known vulnerabilities
- **Trivy**: Multi-purpose security scanner for vulnerabilities and misconfigurations
- **Dependabot**: Automated dependency updates with security alerts

### Secret Detection
- **Gitleaks**: Detects hardcoded secrets, passwords, and API keys
- **TruffleHog**: Additional secret scanning with verification
- Runs on: Every commit, Pull Requests, Daily schedule

## Security Best Practices

When contributing to this project:

1. **Never commit secrets**: Do not hardcode passwords, API keys, tokens, or other sensitive information
2. **Keep dependencies updated**: Regularly update dependencies to patch known vulnerabilities
3. **Follow secure coding practices**: Review OWASP secure coding guidelines
4. **Test security controls**: Include security test cases in your contributions
5. **Review security scan results**: Address any security findings before merging

## Security Workflow Integration

All pull requests are automatically scanned for:
- Code vulnerabilities (CodeQL)
- Dependency vulnerabilities (OWASP, Trivy)
- Exposed secrets (Gitleaks, TruffleHog)
- Configuration issues (Trivy)

Security findings are reported in:
- GitHub Security tab (Security Advisories)
- Pull Request checks
- Workflow artifacts

## CVE Response Process

When a CVE is identified:

1. Triage and assessment within 24 hours
2. Patch development and testing
3. Security advisory publication
4. Coordinated vulnerability disclosure
5. Release of patched versions

## Additional Resources

- [Spring Security Policy](https://spring.io/security-policy)
- [OWASP Top 10](https://owasp.org/www-project-top-ten/)
- [CWE Top 25](https://cwe.mitre.org/top25/)
