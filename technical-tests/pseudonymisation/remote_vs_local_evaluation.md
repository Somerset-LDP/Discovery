# Pseudonymisation Architecture: Local vs Remote Service

## Options to Evaluate

### Option 1: Local Encryption with Remote Key Management

- Embedded libraries in each pipeline component
- Central key management service (AWS KMS/Azure Key Vault)
- Local pseudonymisation execution

### Option 2: Remote-Only Encryption Service

- Central microservice/Lambda for all pseudonymisation
- Central key management service (AWS KMS/Azure Key Vault)
- All components call remote service
- Centralized execution and control

## Evaluation

| Aspect                      | Local Encryption                             | Remote Service                             | Winner                     |
|-----------------------------|----------------------------------------------|--------------------------------------------|----------------------------|
| Security & Compliance       | Keys managed centrally (KMS)                 | Keys and logic centralized                 | Remote wins on compliance  |
|                             | Encryption logic distributed                 | Single point of access control             | simplicity                 |
|                             | Multiple key access points                   | All traffic through one service            |                            |
|                             | Need to secure all components                | Easier to audit single service             |                            |
| Determinism & Joinability   | Risk of version mismatches                   | Single implementation guarantees           | Remote wins on consistency |
|                             | Need synchronized library updates            | consistency                                | guarantee                  |
|                             | Challenge ensuring consistency               | All pseudonymisation uses same logic       |                            |
|                             |                                              | No version drift possible                  |                            |
| Performance & Scalability   | No network calls for encryption              | Network round-trip for each operation      | Local wins on latency,     |
|                             | Limited by individual component resources    | Limited by central service capacity        | Remote wins on centralized |
|                             | Each component scales independently          | Need to scale central service for all load | scaling                    |
| Operational Maintainability | Need to update all pipeline components       | Single deployment point                    | Mixed - Remote easier to   |
|                             | Complex deployment coordination              | Easier to manage versions                  | deploy, Local better fault |
|                             | Issue in one component doesn't affect others | Single point of failure affects all        | isolation                  |
| Resilience & Availability   | Distributed - no single point of failure     | Single point of failure                    | Local wins on resilience   |
|                             | Independent of other services                | All pseudonymisation depends on service    |                            |
|                             | Each component can continue if others fail   | Need backup/caching strategy               |                            |
| Auditability & Governance   | Logs fragmented across components            | Logs centralized by default                | Remote wins on audit       |
|                             | Need to collect from multiple sources        | Single source of truth                     | simplicity                 |
|                             | Harder to track usage patterns               | Easy to monitor all usage                  |                            |
| Future-Proofing             | Need to update all components                | Single deployment                          | Remote wins on algorithm   |
|                             | Risk of inconsistent algorithm versions      | All usage gets new algorithm immediately   | consistency                |
|                             | Coordinated rollout required                 | No coordination needed                     |                            |

## Initial Recommendation

Based on current analysis without volume data:

Factors Favoring Remote Service:

- Determinism guarantee (critical for NHS joins) - Single implementation ensures identical pseudonyms across all systems
- Algorithm and key management simplicity - Centralized control over encryption logic and key versions
- Compliance and audit simplicity
- Operational simplicity

Factors Favoring Local Encryption:

- Performance for high-volume processing
- Resilience and availability
- Independent scaling

Given the critical importance of determinism for NHS cross-dataset joins and the need for centralized algorithm/key
management, Remote Service approach is favored for the following reasons:

1. Determinism Guarantee: Single implementation eliminates risk of version mismatches between components
2. Operational Simplicity: Easy deployment of algorithm updates and key rotation across entire system
3. Risk Mitigation: Centralized control reduces complexity of maintaining consistency across distributed components

Resilience & Scaling: While remote services introduce a central dependency, cloud-native
infrastructure (AWS Lambda, multi-AZ deployment, auto-scaling) transforms this into a highly available service with
better resilience characteristics than managing distributed library versions across multiple pipeline components. The
apparent "single point of failure" becomes a "single point of control" with enterprise-grade availability.

## Next Steps

- Prototype both approaches with NHS-scale test data
- Measure latency impact of remote service for typical batch sizes
- Assess operational complexity of maintaining algorithm consistency in local approach
