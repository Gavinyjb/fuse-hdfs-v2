module fuse-hdfs-v2

go 1.16

require (
	bazil.org/fuse v0.0.0-20200524192727-fb710f7dfd05
	fuse-hdfs-v2/hdfslow v0.0.0-00010101000000-000000000000 // indirect
	fuse-hdfs-v2/mfs v0.0.0-00010101000000-000000000000

)

replace fuse-hdfs-v2/mfs => ./mfs

replace fuse-hdfs-v2/hdfslow => ./hdfslow
