module github.com/KZNGroup/go-go-data-lake

require kzn v1.0.0
replace kzn v1.0.0 => ./kzn

go 1.18

require (
	github.com/aws/aws-lambda-go v1.29.0
	github.com/aws/aws-sdk-go v1.43.36
	github.com/xitongsys/parquet-go v1.6.2
	github.com/xitongsys/parquet-go-source v0.0.0-20200817004010-026bad9b25d0
)

require (
	github.com/apache/arrow/go/arrow v0.0.0-20200730104253-651201b0f516 // indirect
	github.com/apache/thrift v0.14.2 // indirect
	github.com/golang/snappy v0.0.3 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/klauspost/compress v1.13.1 // indirect
	github.com/pierrec/lz4/v4 v4.1.8 // indirect
	golang.org/x/net v0.0.0-20220401154927-543a649e0bdd // indirect
	golang.org/x/xerrors v0.0.0-20191204190536-9bdfabe68543 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
	gopkg.in/yaml.v3 v3.0.0-20210107192922-496545a6307b // indirect
)
