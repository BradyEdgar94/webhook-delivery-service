output "cluster_name" {
  value = aws_eks_cluster.main.name
}

output "kubeconfig_command" {
  value = "aws eks update-kubeconfig --region ${var.region} --name ${aws_eks_cluster.main.name}"
}

output "egress_ip" {
  description = "Static source IP of every outbound webhook (for customer allowlists)"
  value       = aws_eip.nat.public_ip
}

output "database_endpoint" {
  value = aws_db_instance.main.address
}

output "ecr_repository_url" {
  value = aws_ecr_repository.hooklane.repository_url
}
