//go:generate go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
//go:generate go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

//go:generate protoc --go_out=../ --go_opt=paths=source_relative --proto_path=./ ./linearize_models.proto

package proto
