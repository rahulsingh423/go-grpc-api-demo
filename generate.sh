#!/bin/bash
#command to generate grpc code from protobuf file greet.proto
protoc greet/greetpb/greet.proto --go_out=plugins=grpc:.

#command to generate grpc code from protobuf file calculator.proto
protoc calculator/calculatorpb/calculator.proto --go_out=plugins=grpc:.

#command to generate grpc code from protobuf file blog.proto
protoc blog/blogpb/blog.proto --go_out=plugins=grpc:.