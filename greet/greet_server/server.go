package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"

	"github.com/rahulsingh/go-grpc-examples/greet/greetpb"
)

type server struct{}

// Greet is the implementation of Greet() rpc service functions on server struct from generated interface GreetServiceServer
// Unary API server implementation
func (s *server) Greet(ctx context.Context, req *greetpb.GreetRequest) (*greetpb.GreetResponse, error) {
	fmt.Printf("Greet fun was invoked with req: %v\n", req)
	// Get the request data (first_name and last_name)
	firstName := req.GetGreeting().GetFirstName()
	lastName := req.GetGreeting().GetLastName()

	//prepare response message and return
	result := "Hello " + firstName + " " + lastName
	res := &greetpb.GreetResponse{
		Result: result,
	}

	return res, nil
}

// GreetManyTimes is the implementation of GreetManyTimes() rpc service function on server struct from interface GreetServiceServer
// Server Streaming API server implementation
func (s *server) GreetManyTimes(req *greetpb.GreetManyTimesRequest, stream greetpb.GreetService_GreetManyTimesServer) error {
	fmt.Printf("GreetManyTimes fun was invoked with req: %v\n", req)
	// Get the request data (first_name and last_name)
	firstName := req.GetGreeting().GetFirstName()
	lastName := req.GetGreeting().GetLastName()

	//prepare stream response message and return as stream and finally if error return nil
	for i := 0; i < 10; i++ {
		result := "Hello " + firstName + " " + lastName + " -> number " + strconv.Itoa(i)
		res := &greetpb.GreetManyTimesResponse{
			Result: result,
		}

		// stream response
		stream.Send(res)
		// sleep just to show stream response working
		time.Sleep(1000 * time.Millisecond)
	}

	// if no error
	return nil

}

// LongGreet is the implementation of LongGreet() rpc service function on server struct from interface GreetServiceServer
// Client Streaming API server implementation
func (s *server) LongGreet(stream greetpb.GreetService_LongGreetServer) error {
	fmt.Printf("LongGreet fun was invoked with a streaming request\n")
	result := ""
	for {
		// Keep of fetching client stream request till it reaches end-of-file
		req, err := stream.Recv()
		if err == io.EOF {
			// we have finished reading the client request stream
			// return the response on the same stream and close the stream
			return stream.SendAndClose(&greetpb.LongGreetResponse{
				Result: result,
			})

		}
		if err != nil {
			log.Fatalf("error while reading cleint stream: %v", err)
		}

		// Get the request data from client stream request and prepare the response
		firstName := req.GetGreeting().GetFirstName()
		lastName := req.GetGreeting().GetLastName()
		result += "Hello " + firstName + " " + lastName + " ! "
	}
}

// GreetEveryone is the implementation of GreetEveryone () rpc service function on server struct from interface GreetServiceServer
// BiDi Streaming API server implementation
func (s *server) GreetEveryone(stream greetpb.GreetService_GreetEveryoneServer) error {
	fmt.Printf("GreetEveryone func was invoked with a streaming request\n")
	// Iterate over all stream message request and send stream response for each request
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			// done reading stream of request
			return nil
		}
		if err != nil {
			log.Fatalf("error while reading cleint stream: %v", err)
			return err
		}

		// Read the request data and send response of each message of request stream
		firstName := req.GetGreeting().GetFirstName()
		result := "Hello " + firstName + "! "

		// send stream response
		sendErr := stream.Send(&greetpb.GreetEveryoneResponse{
			Result: result,
		})

		if sendErr != nil {
			log.Fatalf("error while sending stream data to cleint: %v", sendErr)
			return sendErr
		}
	}
}

// GreetWithDeadLine is the implementation of GreetWithDeadLine () rpc service function on server struct from interface GreetServiceServer
// It is Deadline (timeout) rpc API server implementation
func (s *server) GreetWithDeadLine(ctx context.Context, req *greetpb.GreetWithDeadLineRequest) (*greetpb.GreetWithDeadLineResponse, error) {
	fmt.Printf("GreetWithDeadLine func was invoked with a request: %v\n", req)

	// producing dealy for testing deadline
	// sleep for 3 seconds
	for i := 0; i < 3; i++ {
		// Every time we should check whether client has canceled the request bcus of timeout or not
		if ctx.Err() == context.Canceled {
			// client has canceled the request
			// return deadline exceeded error
			fmt.Println("The client has canceled the request!")
			return nil, status.Error(codes.Canceled, "the client has canceled the request")
		}
		time.Sleep(1 * time.Second)
	}

	// prepare response and return
	firstName := req.GetGreeting().GetFirstName()
	result := "Hello " + firstName
	res := &greetpb.GreetWithDeadLineResponse{
		Result: result,
	}

	return res, nil
}

func main() {
	fmt.Println("**** GRPC SERVER Setup *****")

	// Create TCP connection and do port binding
	// Default port for grpc server is 50051
	lis, err := net.Listen("tcp", "0.0.0.0:50051")
	if err != nil {
		log.Fatalf("Failed to listen tcp: %v", err)
	}

	opts := []grpc.ServerOption{}
	tls := false // Flag to use or not use SSL encryption
	if tls {
		fmt.Println("<<<<< TLS-SSL is enabled in GRPC Server >>>>>")
		// Setup SSL Encryption credentials for gRPC server over TLS
		certFile := "ssl/server.crt" // ssl certificate
		keyFile := "ssl/server.pem"  // server private key
		creds, sslErr := credentials.NewServerTLSFromFile(certFile, keyFile)
		if sslErr != nil {
			log.Fatalf("failed loading SSL certificate: %v", err)
			return
		}

		// create grpc server options to set ssl certificate with it
		// we can use this option to create grpc server with SSL certificate enabled
		opts = append(opts, grpc.Creds(creds))
	}

	// Create GRPC server and register gRPC serveice with it
	// s := grpc.NewServer() // grpc server without SSL certificate
	s := grpc.NewServer(opts...) // grpc server with SSL certificate [... is used to convert array to arguments ]
	greetpb.RegisterGreetServiceServer(s, &server{})

	// Bind port with grpc server
	if err := s.Serve(lis); err != nil {
		log.Fatalf("Failed to server: %v", err)
	}

}
