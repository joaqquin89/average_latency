package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/go-gota/gota/dataframe"
)

func validateDate(startDate string, endDate string) []string {
	//we try to validate the dates and if the dates are ok .. return an list string
	var dates []string
	datestart, _ := time.Parse("2006-01-02", startDate)
	datend, _ := time.Parse("2006-01-02", endDate)
	start_year := strings.Split(startDate, "-")
	end_year := strings.Split(endDate, "-")
	//the first step is to verify if the year is 2021
	if start_year[0] == "2021" && end_year[0] == "2021" {
		if datend.After(datestart) {
			for d := datestart; d.After(datend) == false; d = d.AddDate(0, 0, 1) {
				dates = append(dates, d.Format("2006-01-02"))
			}
		}
	}
	return dates
}

func unique(results [][]map[string]interface{}, duplicated []map[string]interface{}) []map[string]interface{} {
	//this function try to dedup the results interface and at the end of the code add the records but deleting the record duplicated
	var unique []map[string]interface{}
	for _, v := range results {
		for _, j := range v {
			skip := false
			for _, u := range duplicated {
				if j["requestId"] == u["requestId"] && j["serviceId"] == u["serviceId"] {
					skip = true
					break
				}
			}
			if !skip {
				unique = append(unique, j)
			}
		}
	}
	//in this part we want to verify if exists duplicated into duplicated array, to maintain the consistent
	var unique_dups []map[string]interface{}
	for i := 0; i < len(duplicated); i++ {
		skip2 := true
		for j := i + 1; j < len(duplicated); j++ {
			if duplicated[i]["requestId"] == duplicated[j]["requestId"] {
				skip2 = false
				break
			}
		}
		if skip2 {
			unique_dups = append(unique_dups, duplicated[i])
		}

	}
	for _, v := range unique_dups {
		unique = append(unique, v)
	}

	return unique
}

func getRequests(dates []string) [][]map[string]interface{} {
	//this function is a loop that get the requests between the dates stablished in the request
	var result [][]map[string]interface{}
	for _, v := range dates {
		res, _ := http.Get("http://latencyapi-env.eba-kqb2ph3i.eu-west-1.elasticbeanstalk.com/latencies?date=" + v)
		body, _ := io.ReadAll(res.Body)
		var result_date []map[string]interface{}
		if err := json.Unmarshal([]byte(body), &result_date); err != nil {
			log.Fatalf("unmarshal failed: %s", err)
		}
		result = append(result, result_date)
	}
	return result
}

type json_complete struct {
	dates string
}

func ParsingJson(result [][]map[string]interface{}) []map[string]interface{} {
	//this function is going to generate an list with the duplicated values
	newSlice := make([]map[string]interface{}, 0)
	for _, v := range result {
		for i := 0; i < len(v); i++ {
			valStr := fmt.Sprint(v[i]["requestId"])
			for j := i + 1; j < len(v); j++ {
				if valStr == fmt.Sprint(v[j]["requestId"]) && fmt.Sprint(v[i]["serviceId"]) == fmt.Sprint(v[j]["serviceId"]) {
					newSlice = append(newSlice, v[i])
				}
			}
		}
	}
	//in thos part we want to call the unique function. unique function is going to dedup values in the map
	b, _ := json.Marshal(unique(result, newSlice))
	//here we are going to work with dataframe library to format the output
	df := dataframe.ReadJSON(strings.NewReader(string(b)))

	//sort by service id
	sorted := df.Arrange(dataframe.Sort("serviceId"))
	groups := sorted.GroupBy("serviceId").Aggregation([]dataframe.AggregationType{dataframe.Aggregation_MEAN, dataframe.Aggregation_COUNT}, []string{"milliSecondsDelay", "serviceId"})
	return groups.Rename("averageResonseTimeMs", "milliSecondsDelay_MEAN").Rename("numberOfRequests", "serviceId_COUNT").Maps()
}

//This funtion to manage the requests for our microservice
func helloHandler(w http.ResponseWriter, req *http.Request) {
	// we are getting the start date and end date provided by our url
	startDate := req.URL.Query().Get("startDate")
	endDate := req.URL.Query().Get("endDate")
	dates := validateDate(startDate, endDate)
	//we need to validate if the year is correct or if the date is correct
	if len(dates) > 0 {
		aux := getRequests(dates)
		return_from_parsing := ParsingJson(aux)
		var obj map[string]interface{}
		var intermediate_json []map[string]interface{}
		err := json.Unmarshal([]byte("{}"), &obj)
		if err != nil {
			fmt.Println(err)
			return
		}
		obj["averageLatencies"] = return_from_parsing
		obj["period"] = "[" + startDate + "," + endDate + "]"
		intermediate_json = append(intermediate_json, obj)
		var prettyJSON bytes.Buffer
		final_json, _ := json.Marshal(intermediate_json)
		json.Indent(&prettyJSON, final_json, "", "\t")
		fmt.Fprintf(w, string(prettyJSON.Bytes()))

	} else {
		fmt.Fprintf(w, "WRONG DATES OR WRONG YEAR, THE YEAR IS ONLY 2021")
	}
}

func main() {

	http.HandleFunc("/latencies", helloHandler)
	log.Println("Listing for requests at http://localhost:8000")
	log.Fatal(http.ListenAndServe(":8000", nil))
}
