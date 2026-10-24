# Source ledger

Opened 2026-10-24. Live Kubernetes pages display a v1.37 documentation selector; this is an observed page banner, not a chosen or installed cluster release. Helm pages advertise Helm 4. Spring Boot live reference is 4.1.2; proposed course Boot remains 4.1.1 with Java 21/Maven. No cluster, Helm, or Spring Cloud compatibility matrix was validated.

| ID | Official source | Sections |
|---|---|---|
| K1 | [Components](https://kubernetes.io/docs/concepts/overview/components/) | API server, etcd, scheduler, controllers, kubelet |
| K2 | [Deployments](https://kubernetes.io/docs/concepts/workloads/controllers/deployment/) | ReplicaSets, rolling updates, progress |
| K3 | [StatefulSets](https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/) | Identity and storage |
| K4 | [DaemonSet](https://kubernetes.io/docs/concepts/workloads/controllers/daemonset/) | Node-oriented workloads |
| K5 | [Jobs](https://kubernetes.io/docs/concepts/workloads/controllers/job/) | Completion and retry behavior |
| K6 | [Services](https://kubernetes.io/docs/concepts/services-networking/service/) | Selectors, endpoints, exposure |
| K7 | [Cluster DNS](https://kubernetes.io/docs/concepts/services-networking/dns-pod-service/) | Service and Pod names |
| K8 | [Ingress](https://kubernetes.io/docs/concepts/services-networking/ingress/) | Controller requirement, HTTP routing |
| K9 | [Persistent volumes](https://kubernetes.io/docs/concepts/storage/persistent-volumes/) | PV/PVC, classes, reclaim policy |
| K10 | [Probes](https://kubernetes.io/docs/concepts/workloads/pods/probes/) | Startup, readiness, liveness |
| K11 | [Resources](https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/) | Requests, limits, CPU and memory |
| K12 | [RBAC](https://kubernetes.io/docs/reference/access-authn-authz/rbac/) | Roles and bindings |
| K13 | [Horizontal autoscaling](https://kubernetes.io/docs/concepts/workloads/autoscaling/horizontal-pod-autoscale/) | Metrics and scaling loop |
| K14 | [NetworkPolicy](https://kubernetes.io/docs/concepts/services-networking/network-policies/) | Selection, ingress/egress, implementation |
| K15 | [Helm charts](https://helm.sh/docs/topics/charts/) | Chart structure, templates, values |
| K16 | [Boot Actuator endpoints](https://docs.spring.io/spring-boot/reference/actuator/endpoints.html) | Kubernetes probes and health groups |

Documented mechanisms are cited. Commerce deployment choices, resource values, rollout policies, and experiments are course recommendations. No book consultation or measured cluster results are claimed.


## Evidence boundaries
Only named sections and retrieved content support documented facts. Designs, examples, diagrams, exercises, and arithmetic are original educational synthesis. No books, installed versions, infrastructure actions, executions, or measured results are claimed. Verified date: 2026-10-24.

