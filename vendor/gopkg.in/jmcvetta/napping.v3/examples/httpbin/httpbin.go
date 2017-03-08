// Example demonstrating use of package napping, with HTTP Basic
// Get over HTTP, to exploit http://httpbin.org/.

//
// Implemented the following as demo:
//
// Get -> http://httpbin.org/user-agent
// Get -> http://httpbin.org/get?var=12345

package main

import (
	"fmt"
	"gopkg.in/jmcvetta/napping.v3"
	"log"
)

func init() {
	log.SetFlags(log.Ltime | log.Lshortfile)
}

type ResponseUserAgent struct {
	Useragent string `json:"user-agent"`
}

// A Params is a map containing URL parameters.
type Params map[string]string

func main() {
	//
	// Struct to hold error response
	//
	e := struct {
		Message string
	}{}

	// Start Session
	s := napping.Session{}
	url := "http://httpbin.org/user-agent"
	fmt.Println("URL:>", url)

	res := ResponseUserAgent{}
	resp, err := s.Get(url, nil, &res, nil)
	if err != nil {
		log.Fatal(err)
	}
	//
	// Process response
	//
	println("")
	fmt.Println("response Status:", resp.Status())

	if resp.Status() == 200 {
		// fmt.Printf("Result: %s\n\n", resp.response)
		// resp.Unmarshal(&e)
		fmt.Println("res:", res.Useragent)
	} else {
		fmt.Println("Bad response status from httpbin server")
		fmt.Printf("\t Status:  %v\n", resp.Status())
		fmt.Printf("\t Message: %v\n", e.Message)
	}
	fmt.Println("--------------------------------------------------------------------------------")
	println("")

	url = "http://httpbin.org/get"
	fmt.Println("URL:>", url)
	p := napping.Params{"foo": "bar"}.AsUrlValues()

	res = ResponseUserAgent{}
	resp, err = s.Get(url, &p, &res, nil)
	if err != nil {
		log.Fatal(err)
	}
	//
	// Process response
	//
	println("")
	fmt.Println("response Status:", resp.Status())
	fmt.Println("--------------------------------------------------------------------------------")
	fmt.Println("Header")
	fmt.Println(resp.HttpResponse().Header)
	fmt.Println("--------------------------------------------------------------------------------")
	fmt.Println("RawText")
	fmt.Println(resp.RawText())
	println("")
}
