docker-registry-driver-sohustorage
============

# 概述

docker-registry-driver-sohustorage项目以官方github.com/docker/distribution项目为基础，修改/增添了如下功能：

- 默认使用搜狐云存储作为镜像仓库的存储后端
- 默认打开镜像删除功能
- 可选择使用MySQL或本地文件进行镜像列表查询

注意：

1. 项目使用github.com/docker/distribution的release2.3版本为基础，镜像地址为:pub.domeos.org/domeos/docker-registry-driver-sohustorage:1.1
2. 官方说明文档见README_OFFICIAL.md

# 启动方式

使用docker容器方式启动registry服务：

```bash
sudo docker run --restart=always -d \
  -p <_registry_port>:5000 \
  -e REGISTRY_STORAGE_SCS_ACCESSKEY=<_scs_access_key> \
  -e REGISTRY_STORAGE_SCS_SECRETKEY=<_scs_secret_key> \
  -e REGISTRY_STORAGE_SCS_REGION=<_scs_region> \
  -e REGISTRY_STORAGE_SCS_BUCKET=<_scs_bucket> \
  -e REGISTRY_HTTP_SECRET=<_http_secret> \
  -e SEARCH_BACKEND=<_search_backend> \
  -e SEARCH_BACKEND_MYSQL="<_db_user>:<_db_passwd>@tcp(<_db_addr>)/<_db_database>?loc=Local&parseTime=true"
  --name private-registry \
  pub.domeos.org/domeos/docker-registry-driver-sohustorage:1.1
```

其中参数说明如下：

- _registry_port：私有仓库对外服务端口
- _scs_access_key：搜狐云台 access key
- _scs_secret_key：搜狐云台 secret key
- _scs_region：搜狐云台地域(bjcnc,bjctc,shctc,bjcnc-internal,bjctc-internal,shctc-internal)
- _scs_bucket：搜狐云台桶名，要求该桶存在且可正常访问
- _http_secret：若需启动多个registry容器进行负载均衡，需要配置此环境变量，且赋予相同的值
- _search_backend：查询镜像列表的方式，可选MYSQL或LOCAL，不配置时使用默认的查询方式
- _db_user：DomeOS数据库用户
- _db_passwd：DomeOS数据库密码
- _db_addr：DomeOS数据库地址，IP:Port形式
- _db_database：DomeOS数据库名称

# 说明

- 需正确配置_scs_access_key、_scs_secret_key、_scs_region、_scs_bucket四项参数，否则registry服务无法正常启动；

- _scs_bucket可以为新创建的桶，也可以为已存储镜像的桶；

- SEARCH_BACKEND为MYSQL时，需配置SEARCH_BACKEND_MYSQL为DomeOS的MySQL地址；当registry启动时，将创建repositories表(若repositories表不存在）、
清空repositories表，并将当前registry中已经存在的镜像名称写入repositories表；当向registry上传镜像时，将向repositories表中写入镜像名称和创建时间。
查询registry的catalog时默认将从MySQL中查询。

- repositories表形式：

```bash
CREATE TABLE `repositories` (
	`name` VARCHAR(1024) NOT NULL,
	`createTime` BIGINT(20) NOT NULL DEFAULT '0',
	PRIMARY KEY (`name`)
)
COLLATE='utf8_general_ci'
ENGINE=InnoDB
;
```

- SEARCH_BACKEND为LOCAL时，镜像列表将存储于容器内的/opt/registry/repositories文件内。建议在启动registry容器时将这一文件作为volume，以持久化存储：
-v /path/to/repositories:/opt/registry/repositories，启动镜像前，/path/to/repositories文件必须在宿主机上存在。

- 也可不配置SEARCH_BACKEND，此时使用默认的获取catalog方式。

- 删除镜像操作：DELETE /v2/<name>/manifests/<reference> (参见 https://github.com/docker/distribution/blob/master/docs/spec/api.md )。注意：
删除镜像仅删除相关tag和关联信息，不会删除镜像各层本身。

- 注意：若需启动多个相同的registry容器进行负载均衡，需确保所有容器除暴露端口外的启动参数完全相同(相同的存储后端、相同的search backend)，且启动参数中需要添加REGISTRY_HTTP_SECRET环境变量，设为一致。