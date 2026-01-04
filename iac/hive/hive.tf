resource "helm_release" "postgresql" {
  name      = "postgresql"
  namespace = "bd-bd-gr-02"

  repository = "https://charts.bitnami.com/bitnami"
  chart      = "postgresql"
  version    = "12.1.5"

  values = [file("${path.module}/postgresql-values.yaml")]
}

resource "kubernetes_manifest" "hive_pvc" {
  manifest   = yamldecode(file("${path.module}/hive-pvc.yaml"))
  depends_on = [helm_release.postgresql]
}

resource "kubernetes_manifest" "hive_deployment" {
  manifest   = yamldecode(file("${path.module}/hive-deployment.yaml"))
  depends_on = [kubernetes_manifest.hive_pvc]
}

resource "kubernetes_manifest" "hive_service" {
  manifest   = yamldecode(file("${path.module}/hive-service.yaml"))
  depends_on = [kubernetes_manifest.hive_pvc]
}

resource "kubernetes_manifest" "hive_metastore_entrypoint" {
  manifest   = yamldecode(file("${path.module}/hive-metastore-entrypoint.yaml"))
  depends_on = [kubernetes_manifest.hive_deployment]
}

resource "kubernetes_manifest" "hive_metastore_config" {
  manifest   = yamldecode(file("${path.module}/hive-metastore-config.yaml"))
  depends_on = [kubernetes_manifest.hive_deployment]
}

resource "kubernetes_manifest" "hive_metastore_deployment" {
  manifest   = yamldecode(file("${path.module}/hive-metastore-deployment.yaml"))
  depends_on = [kubernetes_manifest.hive_deployment]
}

resource "kubernetes_manifest" "hive_metastore_service" {
  manifest   = yamldecode(file("${path.module}/hive-metastore-service.yaml"))
  depends_on = [kubernetes_manifest.hive_deployment]
}
