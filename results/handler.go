package results

import (
	"fmt"
	"log"

	"github.com/DistCodeP7/distcode_worker/types"
)

func Handle(results <-chan types.JobResult) {
	for result := range results {
		var output string
		output += "--- Job " + fmt.Sprint(result.JobID) + " Result ---\n"
		if result.Result.Err != nil {
			output += "Error: " + result.Result.Err.Error() + "\n"
		}
		if result.Result.Stdout != "" {
			output += "Stdout: " + result.Result.Stdout + "\n"
		}
		if result.Result.Stderr != "" {
			output += "Stderr: " + result.Result.Stderr + "\n"
		}
		output += "--------------------"
		log.Print(output)
	}
}
