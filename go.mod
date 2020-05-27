module github.com/paypal/gorealis

go 1.13

require (
	github.com/apache/thrift v0.13.0
	github.com/davecgh/go-spew v1.1.0
	github.com/pkg/errors v0.9.1
	github.com/pmezard/go-difflib v1.0.0
	github.com/samuel/go-zookeeper v0.0.0-20171117190445-471cd4e61d7a
	github.com/stretchr/testify v1.2.0
)

replace github.com/apache/thrift v0.13.0 => github.com/ridv/thrift v0.13.2
