variable "region" {
  type = string
}

variable "tags" {
  type = map()
}

variable "boostrap_servers" {
  type = list(string)
}

variable "log_group" {
  type = string
}

variable "worker_configurations" {
  type = object(map)
}

variable "connector_configurations" {
  type = object(map)
}
