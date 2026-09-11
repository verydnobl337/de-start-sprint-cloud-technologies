# Helm deployment

The upstream project contains Helm chart definitions for the three services. They are intentionally separated from application code so container images can be promoted independently.

Recommended production additions: Kubernetes Secrets, resource requests/limits, liveness/readiness probes, PodDisruptionBudget, HorizontalPodAutoscaler and NetworkPolicy.
