# DevSecOps Pipeline Documentation

## Overview

This document describes the DevSecOps implementation for the Spring Kafka project. Security is integrated throughout the development lifecycle with automated scanning, testing, and reporting.

## Security Scanning Tools

### 1. CodeQL (SAST - Static Application Security Testing)

**Purpose**: Analyzes source code to identify security vulnerabilities and coding errors.

**Configuration**: `.github/workflows/codeql-analysis.yml`

**Runs on**:
- Push to main and maintenance branches
- Pull requests
- Daily scheduled scan (2 AM UTC)

**Languages**: Java, Kotlin

**Queries**: Security-extended and security-and-quality rule sets

**Reports to**: GitHub Security tab (Code scanning alerts)

### 2. OWASP Dependency Check

**Purpose**: Identifies known vulnerabilities in project dependencies.

**Configuration**:
- Workflow: `.github/workflows/dependency-check.yml`
- Gradle plugin: `build.gradle` (lines 25, 89-99)
- Suppressions: `config/dependency-check-suppressions.xml`

**Runs on**:
- Push to main and maintenance branches
- Pull requests
- Weekly scheduled scan (Monday 3 AM UTC)

**Key Features**:
- Scans all Gradle dependencies
- CVE database checks
- CVSS score threshold: 7.0 (High)
- Generates HTML, JSON, and XML reports

**Manual execution**:
```bash
./gradlew dependencyCheckAggregate
```

**Reports location**: `build/reports/dependency-check-report.*`

### 3. Trivy Security Scanner

**Purpose**: Multi-purpose vulnerability scanner for dependencies, configurations, and secrets.

**Configuration**: `.github/workflows/trivy-security-scan.yml`

**Runs on**:
- Push to main and maintenance branches
- Pull requests
- Weekly scheduled scan (Tuesday 4 AM UTC)

**Scan types**:
- Vulnerability detection (dependencies)
- Secret detection (credentials, API keys)
- Configuration scanning (misconfigurations)

**Severity levels**: CRITICAL, HIGH, MEDIUM, LOW

**Reports to**:
- GitHub Security tab (SARIF format)
- Workflow artifacts (JSON and table formats)

### 4. Secret Scanning

**Purpose**: Detects exposed secrets, credentials, and sensitive information.

**Configuration**: `.github/workflows/secret-scanning.yml`

**Tools**:
- **Gitleaks**: Pattern-based secret detection
- **TruffleHog**: Secret verification with entropy analysis

**Runs on**:
- Push to main and maintenance branches
- Pull requests
- Daily scheduled scan (5 AM UTC)

**Detection includes**:
- API keys
- Passwords
- Private keys
- Tokens
- Database credentials

### 5. Pull Request Security Checks

**Purpose**: Fast security validation for pull requests.

**Configuration**: `.github/workflows/pr-security-checks.yml`

**Checks**:
- Quick secret scan (Gitleaks)
- High/Critical vulnerability scan (Trivy)
- Configuration validation

**Runs on**: Every pull request

## Security Workflow Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     Developer Workflow                       │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    Pull Request Created                      │
└─────────────────────────────────────────────────────────────┘
                              │
                    ┌─────────┴─────────┐
                    ▼                   ▼
        ┌───────────────────┐  ┌───────────────────┐
        │  PR Build Check   │  │ PR Security Check  │
        │  (Spring CI)      │  │ - Gitleaks         │
        └───────────────────┘  │ - Trivy Quick      │
                               └───────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                      Merge to Main                           │
└─────────────────────────────────────────────────────────────┘
                              │
        ┌─────────────────────┼─────────────────────┐
        ▼                     ▼                     ▼
┌──────────────┐    ┌──────────────┐      ┌──────────────┐
│   CodeQL     │    │    Trivy     │      │   Gitleaks   │
│   Analysis   │    │  Full Scan   │      │ + TruffleHog │
└──────────────┘    └──────────────┘      └──────────────┘
        │                     │                     │
        └─────────────────────┼─────────────────────┘
                              ▼
┌─────────────────────────────────────────────────────────────┐
│           GitHub Security Tab (Centralized View)             │
│  - Code Scanning Alerts                                      │
│  - Dependabot Alerts                                         │
│  - Secret Scanning Alerts                                    │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│              Scheduled Scans (Background)                    │
│  - Daily: CodeQL, Secrets                                    │
│  - Weekly: OWASP Dependency Check, Trivy                     │
└─────────────────────────────────────────────────────────────┘
```

## Security Reporting

### GitHub Security Tab

All security findings are centralized in the GitHub Security tab:

1. **Code scanning alerts** - From CodeQL and Trivy
2. **Dependabot alerts** - Dependency vulnerabilities
3. **Secret scanning alerts** - From Gitleaks and TruffleHog

### Workflow Artifacts

Detailed reports are saved as workflow artifacts:

- **OWASP Dependency Check**: HTML, JSON, XML reports (30 days retention)
- **Trivy**: SARIF and JSON reports (30 days retention)

### Accessing Reports

1. Navigate to the repository's Actions tab
2. Select the workflow run
3. Scroll to "Artifacts" section
4. Download the report archive

## Gradle Tasks

### Run OWASP Dependency Check

```bash
# Analyze all subprojects
./gradlew dependencyCheckAggregate

# View report
open build/reports/dependency-check-report.html
```

### Configure Suppressions

Edit `config/dependency-check-suppressions.xml` to suppress false positives.

Example:
```xml
<suppress>
    <notes><![CDATA[
    False positive: This CVE only affects Windows systems
    Review date: 2025-06-01
    ]]></notes>
    <cve>CVE-2024-12345</cve>
</suppress>
```

## Security Thresholds

| Tool | Severity | Action |
|------|----------|--------|
| CodeQL | Any | Alert in Security tab |
| OWASP Dependency Check | CVSS ≥ 7.0 | Fail build (can be configured) |
| Trivy | CRITICAL, HIGH | Alert in Security tab |
| Gitleaks | Any secret | Fail workflow |

## Best Practices

### For Developers

1. **Run security checks locally** before pushing:
   ```bash
   ./gradlew dependencyCheckAggregate
   ```

2. **Never commit secrets**:
   - Use environment variables
   - Use Spring Boot's `@Value` with external configuration
   - Add sensitive files to `.gitignore`

3. **Keep dependencies updated**:
   - Review Dependabot PRs promptly
   - Update to patched versions when vulnerabilities are found

4. **Review security alerts**:
   - Check GitHub Security tab regularly
   - Address HIGH and CRITICAL findings first

### For Maintainers

1. **Triage security alerts** within 24 hours
2. **Review suppression files** quarterly
3. **Update security workflows** as tools evolve
4. **Monitor false positive rates**
5. **Document security decisions** in `SECURITY.md`

## Scheduled Scans

| Scan Type | Schedule | Day | Time (UTC) |
|-----------|----------|-----|------------|
| CodeQL | Daily | All | 2:00 AM |
| OWASP Dependency Check | Weekly | Monday | 3:00 AM |
| Trivy | Weekly | Tuesday | 4:00 AM |
| Secret Scanning | Daily | All | 5:00 AM |

## Integration with CI/CD

### Pull Request Flow

1. Developer creates PR
2. `pr-build.yml` runs standard build checks
3. `pr-security-checks.yml` runs quick security validation
4. CodeQL analysis runs in background
5. All checks must pass before merge

### Main Branch Flow

1. Code merged to main
2. Full security scan suite runs:
   - CodeQL (SAST)
   - Trivy (vulnerabilities + config)
   - Secret scanning
3. Results published to Security tab
4. Alerts trigger if issues found

### Release Flow

Before release:
1. Review all open security alerts
2. Ensure CRITICAL and HIGH findings are resolved
3. Run full security scan suite
4. Document any accepted risks

## Monitoring and Metrics

Track the following metrics:

- Time to resolve CRITICAL vulnerabilities
- Time to resolve HIGH vulnerabilities
- Number of false positives
- Secret detection rate
- Dependency update frequency

## Support and Resources

- **Security Policy**: `SECURITY.md`
- **Report Security Issues**: security@vmware.com
- **OWASP Top 10**: https://owasp.org/www-project-top-ten/
- **Spring Security**: https://spring.io/guides/gs/securing-web/

## Tool Documentation

- [CodeQL](https://codeql.github.com/docs/)
- [OWASP Dependency Check](https://jeremylong.github.io/DependencyCheck/)
- [Trivy](https://aquasecurity.github.io/trivy/)
- [Gitleaks](https://github.com/gitleaks/gitleaks)
- [TruffleHog](https://github.com/trufflesecurity/trufflehog)

## Maintenance

This DevSecOps pipeline should be reviewed and updated:

- **Quarterly**: Review suppressions and tool configurations
- **Bi-annually**: Evaluate new security tools
- **Annually**: Complete security audit of pipeline

Last updated: 2025-01-04
