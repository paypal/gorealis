package realis

import (
	"fmt"
	"log"
	"net/http"
)

func handler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, `{"roll_forward":true}`)
}

func Run_server() {
	http.HandleFunc("/aurora-update", handler)
	log.Fatal(http.ListenAndServe(":8080", nil))
}
