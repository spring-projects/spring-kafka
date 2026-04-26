# RepoBrain Demo (SDRs + Patterns)

This folder is a lightweight demo of how a repo can ship a "system of record" for engineering intent:

- `repobrain/sdr/spring-kafka-sdrs-and-patterns.txt` contains attributed SDRs (Structured Decision Records)
  extracted from merged PRs, plus an actionable "Pattern" note per SDR.
- `repobrain/context.py` is a tiny helper that answers: "Given a file path, what past decisions/patterns
  likely govern this area?"

## Why this exists

The goal is to prevent architectural drift and regressions by making decisions discoverable and
queryable by humans and AI tools before code is written.

## Quick usage

```bash
python3 repobrain/context.py spring-kafka/src/main/java/org/springframework/kafka/listener/KafkaMessageListenerContainer.java
python3 repobrain/context.py retrytopic
```

## Notes

- This is a demo-only addition on a fork; it does not affect the build.
- The SDR extraction currently relies on public GitHub APIs. For large-scale ingestion, use an
  authenticated token to avoid rate limits.

