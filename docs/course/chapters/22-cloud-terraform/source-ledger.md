# Source ledger

Opened 2026-10-24. Cloud features, regional availability, quotas, prices, service versions, and Terraform provider behavior are time-sensitive; no current price or deployment compatibility is asserted. These rolling official pages support named concepts. Java 21/Maven and proposed Boot 4.1.1 remain unchanged. Terraform and provider versions require a later explicit selection.

| ID | Official source | Sections |
|---|---|---|
| C1 | [AWS shared responsibility](https://aws.amazon.com/compliance/shared-responsibility-model/) | Provider/customer responsibilities |
| C2 | [AWS global infrastructure](https://docs.aws.amazon.com/whitepapers/latest/aws-overview/global-infrastructure.html) | Regions and Availability Zones |
| C3 | [Amazon VPC](https://docs.aws.amazon.com/vpc/latest/userguide/what-is-amazon-vpc.html) | Network, subnets, routing |
| C4 | [IAM practices](https://docs.aws.amazon.com/IAM/latest/UserGuide/best-practices.html) | Roles, temporary credentials, least privilege |
| C5 | [RDS Multi-AZ](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Concepts.MultiAZ.html) | Deployment types and failover |
| C6 | [AWS recovery strategies](https://docs.aws.amazon.com/whitepapers/latest/disaster-recovery-workloads-on-aws/disaster-recovery-options-in-the-cloud.html) | Backup/restore, pilot light, standby, active/active |
| C7 | [AWS Budgets](https://docs.aws.amazon.com/cost-management/latest/userguide/budgets-managing-costs.html) | Monitoring and actions |
| C8 | [Azure regions](https://learn.microsoft.com/en-us/azure/reliability/regions-overview) | Regional design |
| C9 | [Azure Virtual Network](https://learn.microsoft.com/en-us/azure/virtual-network/virtual-networks-overview) | Connectivity and isolation |
| C10 | [Azure managed identities](https://learn.microsoft.com/en-us/entra/identity/managed-identities-azure-resources/overview) | Workload identity |
| C11 | [Azure disaster recovery](https://learn.microsoft.com/en-us/azure/well-architected/reliability/disaster-recovery) | Objectives and testing |
| C12 | [Azure budgets](https://learn.microsoft.com/en-us/azure/cost-management-billing/costs/tutorial-acm-create-budgets) | Budget alerts and actions |
| C13 | [EKS](https://docs.aws.amazon.com/eks/latest/userguide/what-is-eks.html), [AKS](https://learn.microsoft.com/en-us/azure/aks/what-is-aks) | Managed Kubernetes scope |
| C14 | [S3](https://docs.aws.amazon.com/AmazonS3/latest/userguide/Welcome.html), [Blob Storage](https://learn.microsoft.com/en-us/azure/storage/blobs/storage-blobs-introduction) | Object storage |
| T1 | [Terraform state](https://developer.hashicorp.com/terraform/language/state) | Resource bindings and metadata |
| T2 | [State locking](https://developer.hashicorp.com/terraform/language/state/locking) | Concurrent writer protection |
| T3 | [Plan command](https://developer.hashicorp.com/terraform/cli/commands/plan) | Proposed changes and saved plans |
| T4 | [Modules](https://developer.hashicorp.com/terraform/language/modules) | Reusable configuration boundaries |
| T5 | [Sensitive data](https://developer.hashicorp.com/terraform/language/manage-sensitive-data) | Redaction and state exposure |
| T6 | [Provider requirements](https://developer.hashicorp.com/terraform/language/providers/requirements) | Sources and version constraints |
| T7 | [Locals](https://developer.hashicorp.com/terraform/language/values/locals) | Named expressions |

An initial AWS shared-responsibility URL was inaccessible and a recovery URL redirected without useful content; the working official references above replaced them. Microsoft pages exposed substantive text despite a general authorization banner. Architecture choices and examples are original recommendations, not provider guarantees or consulted book content.


## Evidence boundaries
Only named sections and retrieved content support documented facts. Designs, examples, diagrams, exercises, and arithmetic are original educational synthesis. No books, installed versions, infrastructure actions, executions, or measured results are claimed. Verified date: 2026-10-24.

