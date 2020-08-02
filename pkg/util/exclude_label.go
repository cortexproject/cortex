package util

import (
	"errors"
)

type ExcludeLabels map[string]metrics
type metrics []Metric
type Metric struct {
	MetricName string `yaml:"metric_name"`
	LabelName  string `yaml:"label_name"`
}

var (
	errLabelNameMissing  = errors.New("invalid Label Name: Label Name in Exclude Labels Configuration is missing")
	errMetricNameMissing = errors.New("invalid Metric Name: Metric Name in Exclude Labels Configuration is missing")
)

// func (c *Conf) validate() error {
// 	excludeLabels := c.ExcludeLabels
// 	for _, metrics := range excludeLabels {
// 		if len(metrics) != 0 {
// 			for _, metric := range metrics {
// 				if metric.LabelName == "" {
// 					return errLabelNameMissing
// 				}
// 				if metric.MetricName == "" {
// 					return errMetricNameMissing
// 				}
// 			}
// 		}
// 	}
// 	return nil
// }

// func (c *Conf) getConf() *Conf {

// 	yamlFile, err := ioutil.ReadFile("test.yaml")
// 	if err != nil {
// 		log.Printf("yamlFile.Get err   %#v ", err)
// 	}
// 	err = yaml.Unmarshal(yamlFile, c)
// 	if err != nil {
// 		log.Fatalf("Unmarshal: %v", err)
// 	}
// 	c.validate()
// 	return c
// }

// // func main() {

// // 	var c Conf
// // 	c.getConf()

// // 	fmt.Println(c)
// // }
