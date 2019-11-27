package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

// This small tool is executed for each documentation page in the website/content/.
// It will convert all local markdown link into the hugo shortcode relref and convert PR number (e.g #1234)
// to github link.
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
	content := string(buff)
	// fix markdown link by replacing them with relref shortcodes.
	content = convertLinks(content)

	// adds PR links to changelog page
	if strings.Contains(filepath, "CHANGELOG") {
		content = addPRLinks(content)
	}

	if err := ioutil.WriteFile(filepath, []byte(content), 0); err != nil {
		panic(err)
	}
}

var (
	ref    = regexp.MustCompile(`\[(.*)\](\(.*?\))`)
	link   = regexp.MustCompile(`(\(.*?\))`)
	prRef  = regexp.MustCompile(`#(\d+)`)
	images = regexp.MustCompile(`\..*images/(.*)\.(png|gif|jpeg|jpg|pdf)`)
)

func convertLinks(md string) string {
	// grabs relative image path and make then relative to the root of the website where the static images are served.
	// images in the root folder `images/` of this repository are copied over the website/static/images/
	md = images.ReplaceAllStringFunc(md, func(imagePath string) string {
		return fmt.Sprintf("/images/%s", filepath.Base(imagePath))
	})
	// parse markdown link
	return ref.ReplaceAllStringFunc(md, func(ref string) string {
		ref = link.ReplaceAllStringFunc(ref, func(link string) string {
			// we leave out url
			if strings.HasPrefix(link, "(http") {
				return link
			}
			// we leave current page hash tag
			if strings.HasPrefix(link, "(#") {
				return link
			}
			// we leave link already processed
			if strings.HasPrefix(link, "({{<") {
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
}

func addPRLinks(content string) string {
	return prRef.ReplaceAllStringFunc(content, func(ref string) string {
		if len(ref) <= 1 {
			return ref
		}
		ref = ref[1:]
		return fmt.Sprintf("[#%s](https://github.com/cortexproject/cortex/pulls/%s)", ref, ref)
	})
}
