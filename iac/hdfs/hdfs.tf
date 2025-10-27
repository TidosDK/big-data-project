resource "kubernetes_manifest" "hdfs_configmap" {
  manifest = yamldecode(file("${path.module}/configmap.yaml"))
}

resource "kubernetes_manifest" "hdfs_namenodes_deployment" {
  manifest   = yamldecode(file("${path.module}/namenode_deployment.yaml"))
  depends_on = [kubernetes_manifest.hdfs_configmap]
}

resource "kubernetes_manifest" "hdfs_namenodes_service" {
  manifest   = yamldecode(file("${path.module}/namenode_service.yaml"))
  depends_on = [kubernetes_manifest.hdfs_namenodes_deployment]
}

resource "kubernetes_manifest" "hdfs_datanodes_deployment" {
  manifest   = yamldecode(file("${path.module}/datanodes_deployment.yaml"))
  depends_on = [kubernetes_manifest.hdfs_namenodes_deployment]
}

resource "kubernetes_manifest" "hdfs_datanodes_service" {
  manifest   = yamldecode(file("${path.module}/datanodes_service.yaml"))
  depends_on = [kubernetes_manifest.hdfs_datanodes_deployment]
}
