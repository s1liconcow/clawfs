# Routing Decision Log

Persistent few-shot examples for the query router. This file is read before every routing
decision and updated when a cheap-routed query leads to user dissatisfaction. Learned
overrides take priority over the default scoring rubric.

## Stats
- **Created:** 2026-03-18
- **Last updated:** 2026-03-18
- **Total queries routed:** 0
- **Misroutes detected:** 0
- **Override patterns:** 0

## Routing Defaults

### Cheap (opencode) - score <= 3
| Pattern | Example |
|---|---|
| Greetings/acks | "Hi!", "Thanks!", "Yes that works" |
| Factual lookups | "What is DNS?", "Capital of France?" |
| Unit conversions | "Convert 72°F to Celsius" |
| Trivial code | "One-liner to reverse a string in Python" |
| Acronym expansion | "What does ACID stand for?" |
| Simple enumeration | "List 5 sorting algorithms" |
| Direct translation | "Translate 'thank you' to Japanese" |
| Trivial syntax fixes | "Fix: `for i in range(10) print(i)`" |

### Strong (current model) - score >= 4
| Pattern | Example |
|---|---|
| Tradeoff analysis | "Compare Kafka vs Pulsar for event streaming" |
| System design | "Design a multi-region rate limiter" |
| Constrained optimization | "Architect X with <10ms p99 and 99.99% uptime" |
| Deep debugging | "OOM only under high concurrency" |
| Code review/refactor | "Review this PR for security issues" |
| Structured documents | "Write an RFC for encryption at rest" |
| Nuanced policy/strategy | "Second-order effects of AI watermarking" |
| Draft iteration | "Help me iterate on this cover letter" |
| Cross-domain reasoning | "Explain CRDTs then critique the analogy" |

## Learned Overrides

<!-- Entries are appended here automatically when misroutes are detected. -->
<!-- Each entry broadens routing intelligence for future queries. -->
