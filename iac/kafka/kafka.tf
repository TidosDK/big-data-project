resource "helm_release" "kafka_helm_chart" {
  name      = "kafka"
  namespace = var.namespace

  repository = "oci://registry-1.docker.io/bitnamicharts"
  chart      = "kafka"
  version    = "30.0.4"

  values = [file("${path.module}/kafka-values.yaml")]
}

resource "kubernetes_manifest" "kafka_schema_registry_configmap" {
  manifest = yamldecode(file("${path.module}/kafka-schema-registry-configmap.yaml"))
}

resource "kubernetes_manifest" "kafka_schema_registry_deployment" {
  manifest   = yamldecode(file("${path.module}/kafka-schema-registry-deployment.yaml"))
  depends_on = [kubernetes_manifest.kafka_schema_registry_configmap]
}

resource "kubernetes_manifest" "kafka_schema_registry_service" {
  manifest   = yamldecode(file("${path.module}/kafka-schema-registry-service.yaml"))
  depends_on = [kubernetes_manifest.kafka_schema_registry_deployment]
}

resource "kubernetes_manifest" "kafka_connect_pvc" {
  manifest = yamldecode(file("${path.module}/kafka-connect-pvc.yaml"))
}

resource "kubernetes_manifest" "kafka_connect_configmap" {
  manifest   = yamldecode(file("${path.module}/kafka-connect-configmap.yaml"))
  depends_on = [kubernetes_manifest.kafka_connect_pvc]
}

resource "kubernetes_manifest" "kafka_connect_deployment" {
  manifest   = yamldecode(file("${path.module}/kafka-connect-deployment.yaml"))
  depends_on = [kubernetes_manifest.kafka_connect_configmap]
}

resource "kubernetes_manifest" "kafka_connect_service" {
  manifest   = yamldecode(file("${path.module}/kafka-connect-service.yaml"))
  depends_on = [kubernetes_manifest.kafka_connect_deployment]
}
