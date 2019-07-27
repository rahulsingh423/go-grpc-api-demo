package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"

	"github.com/rahulsingh/go-grpc-examples/greet/greetpb"
)

func main() {
	fmt.Println("*** Hello I am in GRPC Client ****")

	// Create a connection to grpc server for given port
	// By default grpc uses SSL security and for that it uses SSL certificate
	// Since we don't have it right now so creating an insecure connection for the time being
	/*	cc, err := grpc.Dial("localhost:50051", grpc.WithInsecure())
		if err != nil {
			log.Fatalf("Could not connect to server: %v", err)
		}
	*/

	tls := false // Flag to use or not use SSL encryption
	opts := grpc.WithInsecure()
	if tls {
		fmt.Println("<<<<< TLS-SSL is enabled in GRPC Client >>>>>")
		// Creating a grpc client with SSL trust certificate
		// create CA trust certificate
		certFile := "ssl/ca.crt" // Certificate Authority Trust Certificate
		creds, sslErr := credentials.NewClientTLSFromFile(certFile, "")
		if sslErr != nil {
			log.Fatalf("error while loading CA trust certificate: %v", sslErr)
			return
		}

		// create cleint options to accept CA Trust Certificate
		opts = grpc.WithTransportCredentials(creds)

	}

	// Create grpc client with SSL CA Trust certificate enabled
	cc, err := grpc.Dial("localhost:50051", opts)
	if err != nil {
		log.Fatalf("Could not connect to server: %v", err)
	}

	// Close connection at the end of program
	defer cc.Close()

	// Create grpc client using connection object
	c := greetpb.NewGreetServiceClient(cc)

	// Making unary rpc call to grpc unary api
	doUnary(c)

	// Making server streaming call to grpc server streaming api
	//doServerStreaming(c)

	// Making client streaming call to grpc client streaming api
	//doClientStreaming(c)

	// Making BiDi streaming call to grpc client streaming api
	//doBiDiStreaming(c)

	// Making Deadline test clients
	//doUnaryCallWithDeadLine(c, 5*time.Second) // should complete as server timeout is 3 seconds

	//doUnaryCallWithDeadLine(c, 1*time.Second) // should timeout as server timeout is 3 seconds

}

// doUnaryCallWithDeadLine implements unary api client with Deadline
func doUnaryCallWithDeadLine(c greetpb.GreetServiceClient, timeout time.Duration) {
	fmt.Printf("Starting doUnaryCallWithDeadLine RPC client....")

	// Prepare GreetWithDeadLineRequest
	req := &greetpb.GreetWithDeadLineRequest{
		Greeting: &greetpb.Greeting{
			FirstName: "Rahul",
			LastName:  "Singh",
		},
	}

	// Calling generated grpc client interface function GreetWithDeadLine()
	// which intern make a grpc call to server and returns GreetWithDeadLineResponse
	// Here context is created with a timeout for timeout duration
	// It returns context and cancel func
	// Whenever this timeout func completes , we cancel the task by defer cancel()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	res, err := c.GreetWithDeadLine(ctx, req)
	if err != nil {
		// When timeout happens we will error which we'll be handling
		// convert error to grpc error and use grpc status code for handline timeout error
		statusErr, ok := status.FromError(err)
		if ok {
			// gRPC error occured
			if statusErr.Code() == codes.DeadlineExceeded {
				// Deadline exceeded error while timeout happens from server
				fmt.Println("Timeout was hit! Deadline was exceeded!")
			} else {
				// for any other gRPC error
				fmt.Printf("unexpected grpc error occured: %v", err)
			}
		} else {
			// Any other fw error
			log.Fatalf("error while calling GreetWithDeadLine RPC: %v", err)
		}
		return
	}
	log.Printf("\nResponse from GreetWithDeadLine() RPC: %v", res.Result)
}

// doBiDiStreaming implements BiDi streaming api client
func doBiDiStreaming(c greetpb.GreetServiceClient) {
	fmt.Printf("Starting doBiDiStreaming RPC client....")

	// we create a stream by invoking the client
	stream, err := c.GreetEveryone(context.Background())
	if err != nil {
		log.Fatalf("error while calling GreetEveryone RPC: %v", err)
	}

	// Creating a slice of requests for request streaming
	requests := []*greetpb.GreetEveryoneRequest{
		&greetpb.GreetEveryoneRequest{
			Greeting: &greetpb.Greeting{
				FirstName: "Rahul",
				LastName:  "Singh",
			},
		},
		&greetpb.GreetEveryoneRequest{
			Greeting: &greetpb.Greeting{
				FirstName: "Chandra",
				LastName:  "Mohan",
			},
		},
		&greetpb.GreetEveryoneRequest{
			Greeting: &greetpb.Greeting{
				FirstName: "Pradeep",
				LastName:  "VVR",
			},
		},
		&greetpb.GreetEveryoneRequest{
			Greeting: &greetpb.Greeting{
				FirstName: "Nishant",
				LastName:  "S",
			},
		},
		&greetpb.GreetEveryoneRequest{
			Greeting: &greetpb.Greeting{
				FirstName: "Akhilesh",
				LastName:  "P",
			},
		},
	}

	// Creating a channel to block things (wait)
	waitc := make(chan struct{})

	// we send a bunch of messages to the client (go routine)
	// This function will run in its own go-routine
	// Both sending and receiving go-routines will be running in parallel to each other
	go func() {
		// func to send bunch of messages
		// loop to send multiple messages
		for _, req := range requests {
			fmt.Printf("Sending message: %v\n", req)
			err := stream.Send(req)
			if err != nil {
				log.Fatalf("error while sending message: %v", err)
			}
			time.Sleep(1000 * time.Millisecond)
		}

		// when we are done with sending all streams of messages to client, we'll close the stream
		stream.CloseSend()
	}()

	// we receive a bunch of messages from the client (go routine)
	// This function will run in its own go-routine
	// Both sending and receiving go-routines will be running in parallel to each other
	go func() {
		// func to receive bunch of messages
		// loop to receive multiple messages
		for {
			res, err := stream.Recv()
			if err == io.EOF {
				break
			}

			if err != nil {
				log.Fatalf("error while receing stream message: %v", err)
				break
			}

			// process the response
			fmt.Printf("Received stream response:> %v\n", res.GetResult())
		}

		// exist condition for our waitc condition
		// so that everything will be unblocked when we receive stream closed from server
		close(waitc)
	}()

	// block until everything is done
	<-waitc

}

// doClientStreaming implements client streaming api client
func doClientStreaming(c greetpb.GreetServiceClient) {
	fmt.Printf("Starting doClientStreaming RPC client....")

	// Creating a slice of requests for request streaming
	requests := []*greetpb.LongGreetRequest{
		&greetpb.LongGreetRequest{
			Greeting: &greetpb.Greeting{
				FirstName: "Rahul",
				LastName:  "Singh",
			},
		},
		&greetpb.LongGreetRequest{
			Greeting: &greetpb.Greeting{
				FirstName: "Chandra",
				LastName:  "Mohan",
			},
		},
		&greetpb.LongGreetRequest{
			Greeting: &greetpb.Greeting{
				FirstName: "Pradeep",
				LastName:  "VVR",
			},
		},
		&greetpb.LongGreetRequest{
			Greeting: &greetpb.Greeting{
				FirstName: "Nishant",
				LastName:  "S",
			},
		},
		&greetpb.LongGreetRequest{
			Greeting: &greetpb.Greeting{
				FirstName: "Akhilesh",
				LastName:  "P",
			},
		},
	}

	// Getting client stream from LongGreet() client and we'll send request over this stream object and
	// get response form the some stream obejct.
	stream, err := c.LongGreet(context.Background())
	if err != nil {
		log.Fatalf("error while calling LongGreet: %v", err)
	}

	// we iterate over our slice of requests and send each request individually to server
	for _, req := range requests {
		fmt.Printf("Sending request: %v", req)

		// Sending stream of request to server
		stream.Send(req)
		time.Sleep(100 * time.Millisecond)
	}

	// After all request is sent, will call stream function CloseAndRecv() to acknowledge server that client is done sending req
	// and now expecting response over same stream
	res, err := stream.CloseAndRecv()
	if err != nil {
		log.Fatalf("error while receiving response from LongGreet rpc: %v", err)
	}
	// log the response received
	fmt.Printf("LongGreet RPC reponse: %v", res)

}

// doServerStreaming implements server streaming api client
func doServerStreaming(c greetpb.GreetServiceClient) {
	fmt.Printf("Starting doServerStreaming RPC client....")

	// Prepare GreetRequest
	req := &greetpb.GreetManyTimesRequest{
		Greeting: &greetpb.Greeting{
			FirstName: "Rahul",
			LastName:  "Singh",
		},
	}

	// get stream response using client
	resStream, err := c.GreetManyTimes(context.Background(), req)
	if err != nil {
		log.Fatalf("error while calling GreetManyTimes RPC: %v", err)
	}

	// loop over stream response to get end-of-response
	for {
		msg, err := resStream.Recv() // returns GreetManyresponse as stream
		if err == io.EOF {
			// we have reached to the end of the stream
			break
		}
		if err != nil {
			log.Fatalf("error while reading stream: %v", err)
		}
		// logging response message
		log.Printf("Response from GreetManyTimes: %v", msg.GetResult())
	}

}

// doUnary implements unary api client
func doUnary(c greetpb.GreetServiceClient) {
	fmt.Printf("Starting doUnary RPC client....")

	// Prepare GreetRequest
	req := &greetpb.GreetRequest{
		Greeting: &greetpb.Greeting{
			FirstName: "Rahul",
			LastName:  "Singh",
		},
	}

	// Calling generated grpc client interface function Greet()
	// which intern make a grpc call to server and returns GreetResponse
	res, err := c.Greet(context.Background(), req)
	if err != nil {
		log.Fatalf("error while calling Greet RPC: %v", err)
	}

	log.Printf("\nResponse from Greet() RPC: %v", res.Result)

}
