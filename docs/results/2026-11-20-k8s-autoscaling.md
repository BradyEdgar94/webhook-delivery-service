# Kubernetes autoscaling demo (kind)

> Measured on a local kind cluster inside a Colima VM (6 CPUs ), not production capacity.

Burst: ~100000 events at 3000/s from an in-cluster k6 Job, 64 endpoints. Workers: KEDA Kafka trigger, lagThreshold 500, min 1, max 16, CPU limit 300m each.

| t (s) | k6 | worker replicas (ready/desired) | consumer lag |
|---:|---|---:|---:|
| 0 | running | 1/1 | 15 |
| 5 | running | 3/3 | 5193 |
| 11 | running | 3/3 | 8401 |
| 16 | running | 3/3 | 14548 |
| 22 | running | 6/7 | 19883 |
| 27 | running | 7/7 | 16151 |
| 33 | running | 7/7 | 21143 |
| 38 | running | 11/11 | 14892 |
| 43 | done | 11/11 | 3875 |
| 49 | done | 11/11 | 758 |
| 54 | done | 11/11 | 44 |
| 60 | done | 11/11 | 0 |
| 65 | done | 5/5 | 0 |
| 70 | done | 5/5 | 0 |
| 76 | done | 5/5 | 0 |
| 81 | done | 2/2 | 0 |
| 86 | done | 2/2 | 0 |
| 91 | done | 2/2 | 0 |
| 97 | done | 1/1 | 0 |

**Summary:** peak desired workers 11, peak consumer lag 21143, lag drained at t=60s,
back to 1 worker at t=97s. k6 accepted 77634 events; deliveries: 77634 succeeded of 77634.
