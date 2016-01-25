package main

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"
)


// Simple serial check of getting and setting
func TestTCPSimple(t *testing.T) {
	go serverMain()
        time.Sleep(1 * time.Second) // one second is enough time for the server to start
	name := "test.txt"
	contents := "testing write"
	exptime := 300000
	conn, err := net.Dial("tcp", "localhost:8080")
	if err != nil {
		t.Error(err.Error()) // report error through testing framework
	}

	scanner := bufio.NewScanner(conn)

	// Write a file
	
	
	fmt.Fprintf(conn, "write %v %v %v\r\n%v\r\n", name, len(contents), exptime, contents)
	scanner.Scan() // read first line
	resp := scanner.Text() // extract the text from the buffer
	arr := strings.Split(resp, " ") // split into OK and <version>
	expect(t, arr[0], "OK")
	version, err := strconv.ParseInt(arr[1], 10, 64) // parse version as number
	if err != nil {
		t.Error("Non-numeric version found")
	}

	fmt.Fprintf(conn, "read %v\r\n", name) // try a read now
	scanner.Scan()

	arr = strings.Split(scanner.Text(), " ")
	expect(t, arr[0], "CONTENTS")
	expect(t, arr[1], fmt.Sprintf("%v", version)) // expect only accepts strings, convert int version to string
	expect(t, arr[2], fmt.Sprintf("%v", len(contents)))	
	scanner.Scan()
	expect(t, contents, scanner.Text())
	
	
	//testing cas
	contents = "testing cas"
	// cas a file
	fmt.Fprintf(conn, "cas %v %v %v %v\r\n%v\r\n", name,version, len(contents), exptime, contents)
	scanner.Scan() // read first line
	resp = scanner.Text() // extract the text from the buffer
	
	
	
	arr = strings.Split(resp, " ") // split into OK and <version>
	expect(t, arr[0], "OK")
	ver_check(t,arr[0],fmt.Sprintf("%v", version))
	version, err = strconv.ParseInt(arr[1], 10, 64) // parse version as number
	if err != nil {
		t.Error("Non-numeric version found")
	}


	fmt.Fprintf(conn, "read %v\r\n", name) // try a read now
	scanner.Scan()

	arr = strings.Split(scanner.Text(), " ")
	expect(t, arr[0], "CONTENTS")
	expect(t, arr[1], fmt.Sprintf("%v", version)) // expect only accepts strings, convert int version to string
	expect(t, arr[2], fmt.Sprintf("%v", len(contents)))	
	scanner.Scan()
	expect(t, contents, scanner.Text())
	
	//testing delete
	
	fmt.Fprintf(conn, "delete %v\r\n", name)
	scanner.Scan() // read first line
	resp = scanner.Text() // extract the text from the buffer
	
//	arr = strings.Split(resp, " ") // split into OK and <version>
	expect(t, resp, "OK")
	
	 
	fmt.Fprintf(conn, "read %v\r\n", name) // try a read now
	scanner.Scan()
//        fmt.Print(scanner.Text())
//	arr = strings.Split(scanner.Text(), " ")
	expect(t, scanner.Text(), "ERR_FILE_NOT_FOUND")
	
	// checking command error
	
	fmt.Fprintf(conn, "write %v\r\n%v\r\n", name)
	scanner.Scan() // read first line
	resp = scanner.Text() // extract the text from the buffer
	arr = strings.Split(resp, " ") // split into OK and <version>
	expect(t, arr[0], "ERR_CMD_ERR")
        
       	scanner.Scan() // read first line
	
	// Internal error example
	
	fmt.Fprintf(conn, "write %v %v\r\n%v\r\n", name,"ab",contents)
	scanner.Scan() // read first line
	resp = scanner.Text() // extract the text from the buffer
	arr = strings.Split(resp, " ") // split into OK and <version>
	expect(t, arr[0], "ERR_INTERNAL")
       	scanner.Scan() // read first line
	
	//testing version err
	
	name = "file.txt"
	contents = "checking version error"
	
	fmt.Fprintf(conn, "write %v %v %v\r\n%v\r\n", name, len(contents), exptime, contents)
	scanner.Scan() // read first line
	resp = scanner.Text() // extract the text from the buffer
	arr = strings.Split(resp, " ") // split into OK and <version>
	expect(t, arr[0], "OK")
	version, err = strconv.ParseInt(arr[1], 10, 64) // parse version as number
	if err != nil {
		t.Error("Non-numeric version found")
	}

	fmt.Fprintf(conn, "read %v\r\n", name) // try a read now
	scanner.Scan()

	arr = strings.Split(scanner.Text(), " ")
	expect(t, arr[0], "CONTENTS")
	expect(t, arr[1], fmt.Sprintf("%v", version)) // expect only accepts strings, convert int version to string
	expect(t, arr[2], fmt.Sprintf("%v", len(contents)))	
	scanner.Scan()
	expect(t, contents, scanner.Text())
	
	//testing cas
	contents = "testing cas"
	// cas a file
	fmt.Fprintf(conn, "cas %v %v %v %v\r\n%v\r\n", name,version+1, len(contents), exptime, contents)
	scanner.Scan() // read first line
	resp = scanner.Text() // extract the text from the buffer
	
	arr = strings.Split(resp, " ") // split into OK and <version>
	expect(t, arr[0], "ERR_VERSION")
	
	scanner.Scan() // read first line
	
	for i:=0;i<10;i++ {
	go cuncur_check(t)
	}
	time.Sleep(5000000000)
	
	
	name = "test.txt"
	contents = "testing write"
        fmt.Fprintf(conn, "read %v\r\n", name)// try a read now
	scanner.Scan()
        fmt.Print("\n#",scanner.Text())       
	//arr = strings.Split(scanner.Text(), " ")
	//expect(t, arr[0], "CONTENTS")
	//expect(t, arr[1], fmt.Sprintf("%v", version)) // expect only accepts strings, convert int version to string
	//expect(t, arr[2], fmt.Sprintf("%v", len(contents)))	
	//scanner.Scan()
	//expect(t, contents, scanner.Text())
	
	scanner.Scan()
        fmt.Print("\n",scanner.Text())       
	
	//fmt.Print(version)
}
	




func cuncur_check(t *testing.T) {

       name := "test.txt"
	contents := "testing write"
	exptime := 300000
	conn, err := net.Dial("tcp", "localhost:8080")
	if err != nil {
		t.Error(err.Error()) // report error through testing framework
	}

	scanner := bufio.NewScanner(conn)

	// Write a file
	
	fmt.Fprintf(conn, "write %v %v %v\r\n%v\r\n", name, len(contents), exptime, contents)
	scanner.Scan() // read first line
	resp := scanner.Text() // extract the text from the buffer
	arr := strings.Split(resp, " ") // split into OK and <version>
	expect(t, arr[0], "OK")
	version, err := strconv.ParseInt(arr[1], 10, 64) // parse version as number
	if err != nil {
		t.Error("Non-numeric version found")
	}

	fmt.Fprintf(conn, "read %v\r\n", name) // try a read now
	scanner.Scan()

	arr = strings.Split(scanner.Text(), " ")
	expect(t, arr[0], "CONTENTS")
	expect(t, arr[1], fmt.Sprintf("%v", version)) // expect only accepts strings, convert int version to string
	expect(t, arr[2], fmt.Sprintf("%v", len(contents)))	
	scanner.Scan()
	expect(t, contents, scanner.Text())
	
	
}










// Useful testing function
func ver_check(t *testing.T, a string, b string) {
	if a == b {
		t.Error(fmt.Sprintf("ERROR : Versions are same")) // t.Error is visible when running `go test -verbose`
	}
}

// Useful testing function
func expect(t *testing.T, a string, b string) {
	if a != b {
		t.Error(fmt.Sprintf("Expected %v, found %v", b, a)) // t.Error is visible when running `go test -verbose`
	}
}
























