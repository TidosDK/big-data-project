module "hdfs" {
  source = "./hdfs"
}

module "kafka" {
  source = "./kafka"
}

module "spark" {
  source = "./spark"
}

module "hive" {
  source = "./hive"
}
