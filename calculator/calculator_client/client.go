package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/rahulsingh/go-grpc-examples/calculator/calculatorpb"
)

func main() {
	fmt.Println("*** Hello I am in Calculator-GRPC Client ****")

	// Create a connection to grpc server for given port
	// By default grpc uses SSL security and for that it uses SSL certificate
	// Since we don't have it right now so creating an insecure connection for the time being
	cc, err := grpc.Dial("localhost:50051", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Could not connect to server: %v", err)
	}

	// Close connection at the end of program
	defer cc.Close()

	// Create grpc client using connection object
	c := calculatorpb.NewCalculatorServiceClient(cc)

	// Making unary rpc call to grpc unary api Sum()
	//doSum(c)

	// Server streaming rpc API example
	//doPrimeNumberDecompositionStreaming(c)

	// Client streaming rpc API example
	//doClientStreaming(c)

	//doBiDiStreaming(c)

	// Error handling example
	doErrorUnary(c)

}

func doErrorUnary(c calculatorpb.CalculatorServiceClient) {
	fmt.Println("Starting SquareRoot-doErrorUnary calculator RPC client....")

	// 1. Making a correct call
	doErrorCall(c, 10)

	// 2. Making an error call
	doErrorCall(c, -4)

}

func doErrorCall(c calculatorpb.CalculatorServiceClient, n int32) {
	res, err := c.SquareRoot(context.Background(), &calculatorpb.SquareRootRequest{Number: n})

	// error handling with grpc status code and message
	// use status & codes package for google grpc for error handling in grpc
	// status:> google.golang.org/grpc/status
	// codes:> google.golang.org/grpc/codes
	if err != nil {
		// convert this error to grpc error status
		// if ok is true then we have got gRPC error sent by user
		// if ok is false then we have got some other framework error [not from grpc]
		resErr, ok := status.FromError(err)
		if ok {
			// actual error from gRPC (user error)
			// get the error code and error message and log or take action accordingly based on code
			fmt.Println("Error Message: ", resErr.Message())
			fmt.Println("Error Code: ", resErr.Code())

			if resErr.Code() == codes.InvalidArgument {
				fmt.Println("We probably sent a negative number!")
			}

		} else {
			// then its a framework error (not from user error)
			log.Fatalf("Big error calling SquareRoot: %v\n", err)
		}

	}

	// if we get success result , process response
	fmt.Printf("Result of square-root of %v: %v\n", n, res.GetNumberRoot())
}

func doBiDiStreaming(c calculatorpb.CalculatorServiceClient) {
	fmt.Println("Starting doBiDiStreaming-FindMaximum calculator RPC client....")

	stream, err := c.FindMaximum(context.Background())
	if err != nil {
		log.Fatalf("error while calling FindMaximum RPC: %v", err)
	}

	waitc := make(chan struct{})

	// send go-routine
	go func() {
		numbers := []int32{4, 7, 2, 19, 4, 6, 32}
		for _, number := range numbers {
			fmt.Printf("Sending number: %v\n", number)
			stream.Send(&calculatorpb.FindMaximumRequest{
				Number: number,
			})
			time.Sleep(1000 * time.Millisecond)
		}
		stream.CloseSend()
	}()

	// receive go-routine
	go func() {
		for {
			res, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Fatalf("error while receiving FindMaximum RPC: %v", err)
				break
			}

			maximum := res.GetMaximum()
			fmt.Printf("Received new maximum of... %v\n", maximum)
		}
		close(waitc)
	}()

	<-waitc
}

func doClientStreaming(c calculatorpb.CalculatorServiceClient) {
	fmt.Println("Starting doClientStreaming-doComputeAverage calculator RPC client....")

	// Creating a slice of requests for request streaming
	numbers := []int32{10, 15, 20, 25, 31}

	stream, err := c.ComputeAverage(context.Background())
	if err != nil {
		log.Fatalf("error while calling ComputeAverage RPC: %v", err)
	}

	for _, number := range numbers {
		fmt.Printf("\nSending request: %v", number)

		// Sending stream of request to server
		stream.Send(&calculatorpb.ComputeAverageRequest{
			Number: number,
		})
		time.Sleep(100 * time.Millisecond)
	}

	// After all request is sent, will call stream function CloseAndRecv() to acknowledge server that client is done sending req
	// and now expecting response over same stream
	res, err := stream.CloseAndRecv()
	if err != nil {
		log.Fatalf("error while receiving response from ComputeAverage rpc: %v", err)
	}
	// log the response received
	fmt.Printf("\nThe Average is from RPC reponse: %v", res.GetAverage())

}

func doPrimeNumberDecompositionStreaming(c calculatorpb.CalculatorServiceClient) {
	fmt.Println("Starting doPrimeNumberDecomposition calculator RPC client....")

	// Prepare GreetRequest
	req := &calculatorpb.PrimeNumberDecompositionRequest{
		InputNumber: 12345467890,
	}

	// get stream response using client
	resStream, err := c.PrimeNumberDecomposition(context.Background(), req)
	if err != nil {
		log.Fatalf("error while calling PrimeNumberDecomposition RPC: %v", err)
	}

	// loop over stream response to get end-of-response
	for {
		msg, err := resStream.Recv() // returns PrimeNumberDecompositionResponse as stream
		if err == io.EOF {
			// we have reached to the end of the stream
			break
		}
		if err != nil {
			log.Fatalf("error while reading stream: %v", err)
		}
		// logging response message
		fmt.Println("Response from PrimeNumberDecomposition: ", msg.GetPrimeNumber())
	}

}

func doSum(c calculatorpb.CalculatorServiceClient) {
	fmt.Println("Starting Sum calculator RPC client....")

	// Prepare GreetRequest
	req := &calculatorpb.SumRequest{
		FirstNumber:  50,
		SecondNumber: 100,
	}

	// Calling generated grpc client interface function Greet()
	// which intern make a grpc call to server and returns GreetResponse
	res, err := c.Sum(context.Background(), req)
	if err != nil {
		log.Fatalf("error while calling Sum RPC: %v", err)
	}

	log.Printf("\nResponse from Sum() RPC: %v", res.SumResult)
}
