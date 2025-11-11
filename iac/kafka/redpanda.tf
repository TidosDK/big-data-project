resource "kubernetes_manifest" "redpanda_configmap" {
  manifest = yamldecode(file("${path.module}/redpanda-configmap.yaml"))
}

resource "kubernetes_manifest" "redpanda_deployment" {
  manifest   = yamldecode(file("${path.module}/redpanda-deployment.yaml"))
  depends_on = [kubernetes_manifest.redpanda_configmap]
}

resource "kubernetes_manifest" "redpanda_service" {
  manifest   = yamldecode(file("${path.module}/redpanda-service.yaml"))
  depends_on = [kubernetes_manifest.redpanda_deployment]
}
