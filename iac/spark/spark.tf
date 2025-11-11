resource "helm_release" "spark_helm_chart" {
  name      = "spark"
  namespace = "bd-bd-gr-02"

  repository = "oci://registry-1.docker.io/bitnamicharts"
  chart      = "spark"
  version    = "9.2.10"

  values = [file("${path.module}/spark-values.yaml")]
}
