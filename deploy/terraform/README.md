# Optional: Hooklane on AWS (EKS) for a load test

> **This is never applied by the build or by any `make` target.** It exists to
> show what the cloud version looks like, and was only checked with
> `terraform fmt -check` and `terraform validate`.

What it creates: a VPC (2 public + 2 private subnets), **one NAT gateway with
an Elastic IP** (every outbound webhook leaves from that one static address, so
customers can allowlist it), an EKS cluster with 3 `m6i.xlarge` nodes, an ECR
repository, and a single-AZ RDS Postgres (`db.m6g.large`). Redpanda, KEDA,
metrics-server and Hooklane run inside EKS using
`deploy/k8s/overlays/cloud` (workers scale to 64, one per partition).

## Cost

Rough on-demand prices in us-east-1 (check the AWS pricing pages before running):

| Item | ~USD / hour |
|---|---|
| EKS control plane | 0.10 |
| 3 × m6i.xlarge nodes | 0.58 |
| NAT gateway (+ $0.045 per GB processed) | 0.05 |
| RDS db.m6g.large, single AZ, 100 GB gp3 | 0.17 |
| Public IPv4 addresses, EBS volumes | ~0.03 |
| **Total** | **≈ $0.95 per hour**, plus data transfer |

A two-hour load test costs about $2–3. Left running for a month it would cost
about $700. **Create it for the test and destroy it straight after.**

## Use

```bash
cd deploy/terraform
export TF_VAR_db_password='<a long random password>'
terraform init
terraform apply                      # ~15-20 minutes (EKS is slow to create)
$(terraform output -raw kubeconfig_command)
# push the image to ECR, set the image and RDS endpoint in deploy/k8s/overlays/cloud,
# install KEDA + metrics-server, then: kubectl apply -k ../k8s/overlays/cloud
```

## Destroy (do not skip)

```bash
cd deploy/terraform
terraform destroy -auto-approve
```

Then confirm in the AWS console that no NAT gateway, Elastic IP, EKS cluster or
RDS instance named `hooklane` remains. Load balancers created by Kubernetes
Services are not managed by Terraform: delete the Kubernetes Services of type
LoadBalancer (if you created any) *before* `terraform destroy`.
