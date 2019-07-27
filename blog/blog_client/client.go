package main

import (
	"context"
	"fmt"
	"io"
	"log"

	"google.golang.org/grpc"

	"github.com/rahulsingh/go-grpc-examples/blog/blogpb"
)

func main() {
	fmt.Println("*** Hello I am in Blog-GRPC Client ****")

	opts := grpc.WithInsecure()

	// Create grpc client with SSL CA Trust certificate enabled
	cc, err := grpc.Dial("localhost:50051", opts)
	if err != nil {
		log.Fatalf("Could not connect to server: %v", err)
	}

	// Close connection at the end of program
	defer cc.Close()

	// Create grpc client using connection object
	c := blogpb.NewBlogServiceClient(cc)

	// creating blog client
	fmt.Println("Creating a Blog")
	// create a blog request
	blog := &blogpb.Blog{
		AuthorId: "rahul",
		Title:    "my first blog",
		Content:  "content of first blog",
	}

	createBlogRes, err := c.CreateBlog(context.Background(), &blogpb.CreateBlogRequest{Blog: blog})
	if err != nil {
		log.Fatalf("error in creating blog: %v\n", err)
	}
	fmt.Printf("Blog is created successfully: %v\n", createBlogRes)

	// read blog client
	fmt.Println("Reading a Blog")

	// error case // NOT_FOUND
	_, err2 := c.ReadBlog(context.Background(), &blogpb.ReadBlogRequest{BlogId: "1dhdhfhs"})
	if err2 != nil {
		fmt.Printf("error while reading the blog: %v\n", err2)
	}

	// success case
	blogID := createBlogRes.GetBlog().GetId()
	blogIDReq := &blogpb.ReadBlogRequest{BlogId: blogID}
	res, err2 := c.ReadBlog(context.Background(), blogIDReq)
	if err2 != nil {
		fmt.Printf("error while reading the blog: %v\n", err2)
	}

	fmt.Printf(" Blog was read: %v\n", res)

	// Update the blog
	fmt.Println("Updating a Blog")

	newBlog := &blogpb.Blog{
		Id:       blogID,
		AuthorId: "changed Auther",
		Title:    "changed: my first blog",
		Content:  "changed: content of first blog",
	}

	updateRes, err := c.UpdateBlog(context.Background(), &blogpb.UpdateBlogRequest{Blog: newBlog})
	if err != nil {
		fmt.Printf("error while updating blog: %v\n", err)
	}

	fmt.Printf("Updated the blog successfully: %v\n", updateRes)

	// Delete a Blog
	fmt.Println("Deleting a Blog")
	deleteRes, err := c.DeleteBlog(context.Background(), &blogpb.DeleteBlogRequest{BlogId: blogID})
	if err != nil {
		fmt.Printf("error while deleting blog: %v\n", err)
	}

	fmt.Printf("Deleted the blog successfully for blogID: %v\n", deleteRes)

	// List blog as stream from server
	fmt.Println("<<<<<<List blog as stream from server>>>>>>")

	// get stream response using client
	resStream, err := c.ListBlog(context.Background(), &blogpb.ListBlogRequest{})
	if err != nil {
		log.Fatalf("error while calling List Blog stream RPC: %v", err)
	}

	// loop over stream response to get end-of-response
	for {
		msg, err := resStream.Recv() // returns ListBlogResponse as stream
		if err == io.EOF {
			// we have reached to the end of the stream
			break
		}
		if err != nil {
			log.Fatalf("error while reading stream: %v", err)
		}
		// logging response message
		fmt.Println("Response from ListBlog-Stream: ", msg.GetBlog())
	}

}
