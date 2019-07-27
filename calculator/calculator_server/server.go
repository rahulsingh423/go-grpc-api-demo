package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"math"
	"net"

	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/rahulsingh/go-grpc-examples/calculator/calculatorpb"
)

type server struct{}

// Implement all functions of generated interface CalculatorServiceServer i.e. Sum() in grcp server on server struct
func (s *server) Sum(ctx context.Context, req *calculatorpb.SumRequest) (*calculatorpb.SumResponse, error) {
	fmt.Printf("Sum fun was invoked with req: %v\n", req)
	// Get the request data (first_number, second_number)
	firstNumber := req.GetFirstNumber()
	secondNumber := req.GetSecondNumber()

	sum := (firstNumber + secondNumber)

	res := &calculatorpb.SumResponse{
		SumResult: sum,
	}

	return res, nil
}

func (s *server) PrimeNumberDecomposition(req *calculatorpb.PrimeNumberDecompositionRequest, stream calculatorpb.CalculatorService_PrimeNumberDecompositionServer) error {
	fmt.Printf("PrimeNumberDecomposition fun was invoked with req: %v\n", req)
	// Get the request data (input_number)
	inputNumber := req.GetInputNumber()

	// Get the prime number decomposition of given number and steam the response
	divisor := int64(2)

	for inputNumber > 1 {
		if inputNumber%divisor == 0 {
			stream.Send(&calculatorpb.PrimeNumberDecompositionResponse{
				PrimeNumber: divisor,
			})
			inputNumber = inputNumber / divisor
		} else {
			divisor++
			fmt.Printf("\nDivisor has increased to %v", divisor)
		}
	}

	return nil
}

func (s *server) ComputeAverage(stream calculatorpb.CalculatorService_ComputeAverageServer) error {
	fmt.Printf("ComputeAverage fun was invoked with stream request")
	sum := int32(0)
	count := 0
	for {
		// Keep of fetching client stream request till it reaches end-of-file
		req, err := stream.Recv()
		if err == io.EOF {
			// we have finished reading the client request stream
			// return the response on the same stream and close the stream
			return stream.SendAndClose(&calculatorpb.ComputeAverageResponse{
				Average: float32(sum) / float32(count),
			})
		}

		if err != nil {
			log.Fatalf("error while reading cleint stream: %v", err)
			return err
		}

		// Get the request numbers from steam and calculate their average
		sum += req.GetNumber()
		count++

	}

}

func (s *server) FindMaximum(stream calculatorpb.CalculatorService_FindMaximumServer) error {
	fmt.Printf("FindMaximum fun was invoked with stream request")
	maximum := int32(0)
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			log.Fatalf("error while reading cleint stream: %v", err)
			return err
		}

		number := req.GetNumber()
		if number > maximum {
			maximum = number
			sendErr := stream.Send(&calculatorpb.FindMaximumResponse{
				Maximum: maximum,
			})
			if sendErr != nil {
				log.Fatalf("error while sending stream response: %v", sendErr)
				return sendErr
			}
		}

	}
}

// SquareRoot is used to calculare squareroot of positive number
// Error handling
// This RPC will throw an execpetion if the sent number is negative
// The error being sent if of type INVALID_ARGUMENT
func (s *server) SquareRoot(ctx context.Context, req *calculatorpb.SquareRootRequest) (*calculatorpb.SquareRootResponse, error) {
	fmt.Printf("SquareRoot fun was invoked with request: %v", req)

	// get the request number
	number := req.GetNumber()

	// Input validation and return INVALID_ARGUMENT exception of input is negative
	if number < 0 {
		// use status & codes package for google grpc for error handling in grpc
		// status:> google.golang.org/grpc/status
		// codes:> google.golang.org/grpc/codes
		// send status code as well as error message
		return nil, status.Errorf(
			codes.InvalidArgument,
			fmt.Sprintf("Received a negative number: %v", number),
		)
	}

	// If number is positive, return square root of number
	return &calculatorpb.SquareRootResponse{
		NumberRoot: math.Sqrt(float64(number)),
	}, nil

}

func main() {
	fmt.Println("**** GRPC SERVER Setup *****")

	// Create TCP connection and do port binding
	// Default port for grpc server is 50051
	lis, err := net.Listen("tcp", "0.0.0.0:50051")
	if err != nil {
		log.Fatalf("Failed to listen tcp: %v", err)
	}

	// Create GRPC server and register gRPC serveice with it
	s := grpc.NewServer()
	calculatorpb.RegisterCalculatorServiceServer(s, &server{})

	// Register server with grpc-reflection
	reflection.Register(s)

	// Bind port with grpc server
	if err := s.Serve(lis); err != nil {
		log.Fatalf("Failed to server: %v", err)
	}

}
