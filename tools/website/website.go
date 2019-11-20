package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"regexp"
	"strings"
)

// run through a folder
func main() {

	log.Println(os.Args)
	if len(os.Args) < 2 {
		log.Fatal("provide a file to process")
	}
	filepath := os.Args[1]
	buff, err := ioutil.ReadFile(filepath)
	if err != nil {
		panic(err)
	}
	ref := regexp.MustCompile(`\[(.*)\](\(.*?\))`)
	link := regexp.MustCompile(`(\(.*?\))`)

	fixed := ref.ReplaceAllStringFunc(string(buff), func(ref string) string {
		ref = link.ReplaceAllStringFunc(ref, func(link string) string {
			// we leave out url
			if strings.HasPrefix(link, "http") {
				return link
			}
			// we leave current page hash tag
			if strings.HasPrefix(link, "#") {
				return link
			}
			// we leave link already processed
			if strings.HasPrefix(link, "{{<") {
				return link
			}
			// we leave current any that is not a page ref
			if !strings.Contains(link, ".md") {
				return link
			}
			if len(link) <= 2 {
				return link
			}

			link = strings.Trim(link, "(")
			link = strings.Trim(link, ")")
			return fmt.Sprintf(`({{< relref "%s" >}})`, link)
		})
		return ref
	})
	if err := ioutil.WriteFile(filepath, []byte(fixed), 0); err != nil {
		panic(err)
	}
}
