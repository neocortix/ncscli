package main
import (
    "fmt"
    "os"
    "strconv"
)
func main() {
	frameNum := 0
	if len(os.Args)>1 {
		frameNum, _ = strconv.Atoi(os.Args[1])
        }
	fmt.Printf("Hello, world!  From frameNum = %d \n", frameNum)
}
