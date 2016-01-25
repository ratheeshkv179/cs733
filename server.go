package main

//package assignment1

import ( 
	"fmt"
	"sync"
	"net"
	"strings"
	"bufio"
	"strconv"
	"io"
//	"io/ioutil"
	"time"
	"github.com/syndtr/goleveldb/leveldb"   
	)
 
var mutex = &sync.Mutex{}
var mutex_ver = &sync.Mutex{}
var mutex_rd = &sync.Mutex{}
var mutex_wrt = &sync.Mutex{}
var mutex_cas = &sync.Mutex{}
var mutex_del = &sync.Mutex{}

var db, _ = leveldb.OpenFile("/tmp/file_content.db", nil)
var db_ver, _ = leveldb.OpenFile("/tmp/version.db", nil)
var db_fver, _ = leveldb.OpenFile("/tmp/file_version.db", nil)
var db_expiry, _ = leveldb.OpenFile("/tmp/file_expiry.db", nil)
var db_exp_sec, _ = leveldb.OpenFile("/tmp/file_exp_sec.db", nil)

func serverMain(){

	ip_address, err := net.ResolveTCPAddr("tcp4","localhost:8080")
	if err!=nil {
	} else {
	
//	checkexpiry()
	listen, _  := net.ListenTCP("tcp", ip_address)
	for {
	conn, err := listen.Accept()
	if( err!=nil){
	} else {
	go client(conn)
	}}}
}

/*
func incVersion(){
        mutex.Lock()
        var version int64 = 0
        data,err:= db_ver.Get([]byte("version"), nil)
        if(err != nil) {
        version = 1
        _ = db_ver.Put([]byte("version"), []byte(strconv.FormatInt(version, 10)), nil)
        } else {
        version, _ = strconv.ParseInt(string(data), 10, 64)    
        version = version + 1
        _ = db_ver.Put([]byte("version"), []byte(strconv.FormatInt(version, 10)), nil)
        }
       mutex.Unlock()
}
*/

func getVersion(fname string) (int64) {
        //mu.Lock()
        data,err:= db_fver.Get([]byte(fname), nil)
        //defer mu.Unlock()
        if(err != nil) {
                return -1
        } else {
        version, _ := strconv.ParseInt(string(data), 10, 64)    
        //fmt.Prin("\n++++",version)        
        return version        
        }
}

func setVersion(  fname string) {
        //fmt.Prin("\n$")
        mutex_ver.Lock()
        //fmt.Prin("$")
//        incVersion()
        var version int64 = 0
        data,err:= db_ver.Get([]byte("version"), nil)
        if(err != nil) {
        version = 1
        _ = db_ver.Put([]byte("version"), []byte(strconv.FormatInt(version, 10)), nil)
        } else {
        version, _ = strconv.ParseInt(string(data), 10, 64)    
        version = version + 1
        _ = db_ver.Put([]byte("version"), []byte(strconv.FormatInt(version, 10)), nil)
        }
//        data,_ = db_ver.Get([]byte("version"), nil)
        //fmt.Prin("\n****",version)
//        ver , _ := strconv.ParseInt(string(version), 10, 64)    
//        //fmt.Prin("\n----",ver)
        err = db_fver.Put([]byte(fname), []byte(strconv.FormatInt(version, 10)), nil)
        //fmt.Prin("\n$!")
        mutex_ver.Unlock()
        //fmt.Prin("$!")
}

/*
func setExpiry(  fname string,   exp int64 ) {

   //       mutex.Lock()
        exp = exp + time.Now().Unix()
        err := db_expiry.Put([]byte(fname), []byte(strconv.FormatInt(exp, 10)), nil)
        if(err != nil) {
        }
//          mutex.Unlock()
} */

func getExpiry(  fname string) (int64){

          data, err := db_expiry.Get([]byte(fname), nil)
       
          if (err != nil) {
          return -1
          } else {
          exp, err := strconv.ParseInt(string(data), 10, 64)        
               if(err !=nil) {
               return -1
               } else {
               return exp
               }
          }
}

func setExpirySec(  fname string,   exp int64) {

   //       mutex_exp.Lock()        
       // mutex.Lock()
       // setExpiry(fname,exp)
        exp = exp + time.Now().Unix()
        err := db_expiry.Put([]byte(fname), []byte(strconv.FormatInt(exp, 10)), nil)
        err = db_exp_sec.Put([]byte(fname), []byte(strconv.FormatInt(exp, 10)), nil)
        if(err != nil) {
        }
        
 //         defer mutex_exp.Unlock()
       //   mutex.Unlock()
}

func getExpirySec(  fname string) (int64) {

     //     mutex.Lock()
          data, err := db_exp_sec.Get([]byte(fname), nil)
      //      mutex.Unlock()
          if (err != nil) {
          return -1
          } else {
          exp, err := strconv.ParseInt(string(data), 10, 64)        
          
               if(err !=nil) {
               return -1
               } else {
               return exp
               }
          }
}
 
/*
func checkexpiry(){
        for true {
        time.Sleep(1000000000)
        
        iter := db.NewIterator(nil, nil)

        for iter.Next() {
        if(iter !=nil) {
        cur_time  := time.Now().Unix()
	// Remember that the contents of the returned slice should not be modified, and
	// only valid until the next call to Next.
	
	if(cur_time >= getExpiry(string(iter.Key()))) {
	db.Delete([]byte(string(iter.Key())), nil)
        db_fver.Delete([]byte(string(iter.Key())), nil)
        db_expiry.Delete([]byte(string(iter.Key())), nil)
        db_exp_sec.Delete([]byte(string(iter.Key())), nil)
	}
	}
	//fmt.Print("\nKEY : ",iter.Key())
//        fmt.Print("\nValue : ",iter.Value())
        }
        iter.Release()
        _ = iter.Error()


        
/*        for k := range expiry 
        cur_time  := time.Now().Unix()
        if(exp_sec[k] != -1 && exp_sec[k]!=0) {
        if (expiry[k]<=cur_time) {
        
//        delete(expiry,k)
//        delete(exp_sec,k)
//        delete(version,k)
        _ = db_exp_sec.Delete([]byte(k), nil)
        _ = db_expiry.Delete([]byte(k), nil)
        _ = db_ver.Delete([]byte("version"), nil)
        _ = db.Delete([]byte(k), nil)
        
        }}
    }}
*/
 

func client(conn net.Conn){

 
       	reader := bufio.NewReader(conn)
        		
	for true {

	data, err := reader.ReadBytes('\n')

	if(err== io.EOF) {
	break
	} else	if (err!=nil){
	fmt.Fprintf(conn,"ERR_INTERNAL\r\n")
	
	} else {
	

	if(data[len(data)-2]!='\r') {
	fmt.Fprintf(conn,"ERR_INTERNAL\r\n")
	} else {
	command := string(data[0:len(data)-2])
	part := strings.Fields(command)



	if((strings.Compare(part[0],"write"))==0) {

	if(len(part)==3 ||len(part)==4){
	numbytes,err := strconv.Atoi(part[2])
	if(err != nil ) { // number of bytes in not a number
	fmt.Fprintf(conn,"ERR_INTERNAL\r\n")
	} else {

        flag := 0
        expiry_time := 0

	if(len(part)==4){            // Expiry time present
	expiry_time,err = strconv.Atoi(part[3])
	if(err != nil ) { // number of bytes in not number
	flag = 1
	fmt.Fprintf(conn,"ERR_INTERNAL\r\n")
	}}
	
       if ( flag == 0){
	 
	data, err := reader.ReadBytes('\n')
	var content [] byte
	for (data[len(data)-2] != '\r') {
	content = append(content,data...)
	data, err = reader.ReadBytes('\n')
	}
	content = append(content,data[:len(data)-2]...)
	if(len(content)!=numbytes) {
	fmt.Fprintf(conn,"ERR_INTERNAL\r\n")
	} else {
	
	//fmt.Prin("\n@")
        mutex.Lock()	
	//fmt.Prin("@")
	
        err = db.Put([]byte(string(part[1])), []byte(content), nil)
        
        //fmt.Prin("\n@!")
        mutex.Unlock()	
        //fmt.Prin("@!")	
        
	if (err!=nil) {
	fmt.Fprintf(conn,"ERR_INTERNAL\r\n")
	} else {
	
	//fmt.Prin("\n#")
	mutex.Lock()	
	//fmt.Prin("#")	
        setVersion(string(part[1]))
        //fmt.Prin("\nVersion : ",getVersion(string(part[1])))
        fmt.Fprintf(conn,"OK %v\r\n",getVersion(string(part[1])))	              
        
	if (len(part)==4) {
	setExpirySec(string(part[1]),int64(expiry_time))
        } else {
        	setExpirySec(string(part[1]),int64(-1))
        }
        //fmt.Prin("\n#!")
        mutex.Unlock()
        //fmt.Prin("#!")
        }
     //   mutex.Unlock()		
	content = nil
        }}}
        } else {
        fmt.Fprintf(conn,"ERR_CMD_ERR\r\n")
        }
        
        }  else if ((strings.Compare(part[0],"cas"))==0) {

	if((len(part)==4 ||len(part)==5) && (len(part[1])<=256)){
      	_, err := db.Get([]byte(string(part[1])), nil)
	if(err != nil){
	fmt.Fprintf(conn,"ERR_FILE_NOT_FOUND\r\n")	
	} else {
	versn,err := strconv.Atoi(part[2])
	if(err != nil ) { // version in not a number
	fmt.Fprintf(conn,"ERR_INTERNAL\r\n")
	} else {
	numbytes,err := strconv.Atoi(part[3])
	if(err != nil ) { // number of bytes in not a number
	fmt.Fprintf(conn,"ERR_INTERNAL\r\n")
	} else {
	if(getVersion(string(part[1])) != int64(versn)) {
	fmt.Fprintf(conn,"ERR_VERSION\r\n")
	} else {
	flag := 0
	exp := 0
	if(len(part)==5) {
	exp,err = strconv.Atoi(part[4])
	if(err != nil ) { // number of bytes in not a number
	fmt.Fprintf(conn,"ERR_INTERNAL\r\n")
	flag = 1
	}}
	if (flag == 0){
	data, _ := reader.ReadBytes('\n')
	var content [] byte
	for (data[len(data)-2] != '\r') {
	content = append(content,data...)
	data, err = reader.ReadBytes('\n')
	}
	content = append(content,data[:len(data)-2]...)
        
	if(len(content)!=numbytes) {
	fmt.Fprintf(conn,"ERR_INTERNAL\r\n")
	} else {
	
//	mutex.Lock()
	//delete
	mutex_cas.Lock()
        err = db.Put([]byte(string(part[1])), []byte(content), nil)
        mutex_cas.Unlock()
        
	if (err!=nil) {
	fmt.Fprintf(conn,"ERR_INTERNAL\r\n")
	} else {
	
        mutex_cas.Lock()
	if (len(part)==4) {
	setExpirySec(string(part[1]),int64(-1))
//        exp_sec[part[1]]	= -1
	} else {
	setExpirySec(string(part[1]),int64(exp))
//	exp_sec[part[1]] = int64(exp)
//        expiry[part[1]]	=  int64(exp) + time.Now().Unix()
	}
	setVersion(string(part[1]))
        fmt.Fprintf(conn,"OK %v\r\n",getVersion(string(part[1])))	              
        
        mutex_cas.Unlock()
	}
//	
	content = nil
	//file.Close()
	}}}}}}
	} else {
	fmt.Fprintf(conn,"ERR_CMD_ERR\r\n")
	}
	
	
	} else if ((strings.Compare(part[0],"read"))==0) {
	
	
	if(len(part)==2 && len(part[1])<=256){

        mutex_rd.Lock()
        //fmt.Prin("\nread1")
        contents, err := db.Get([]byte(string(part[1])), nil)
                //fmt.Prin("\nread2")
//        mutex_rd.Unlock()
        
	//////fmt.Prin("\nFile Name : ",len(string(part[1])))
	if(err !=nil) {
	
	fmt.Fprintf(conn,"ERR_FILE_NOT_FOUND\r\n")
	}else {
	
//	mutex.Lock()
	//////fmt.Prin("\nFile Content :",string(contents))
//	if (exp_sec[string(part[1])] == -1)
        if (getExpirySec(string(part[1])) == -1){
	fmt.Fprintf(conn,"CONTENTS %v %v\r\n%v\r\n",getVersion(string(part[1])),len(contents),string(contents))	
	}else {
	fmt.Fprintf(conn,"CONTENTS %v %v %v\r\n%v\r\n",getVersion(string(part[1])),len(contents),getExpirySec(string(part[1])),string(contents))	
//	mutex.Unlock()
	}}
	mutex_rd.Unlock()
	
	} else {
	fmt.Fprintf(conn,"ERR_CMD_ERR\r\n")
	}
	
	} else if ((strings.Compare(part[0],"delete"))==0) {
	//////fmt.Prin("\nDELETE")
	if(len(part)==2 && len(part[1])<=256){
	
	
        mutex_del.Lock()
        err = db.Delete([]byte(string(part[1])), nil)
        
	if(err != nil) {
	fmt.Fprintf(conn,"ERR_FILE_NOT_FOUND\r\n")
	} else {
        db_fver.Delete([]byte(string(part[1])), nil)
        db_expiry.Delete([]byte(string(part[1])), nil)
        db_exp_sec.Delete([]byte(string(part[1])), nil)
	fmt.Fprintf(conn,"OK\r\n")
	}
        mutex_del.Unlock()
        
	} else {
	fmt.Fprintf(conn,"ERR_CMD_ERR\r\n")
	}
	} else {
	fmt.Fprintf(conn,"ERR_CMD_ERR\r\n")
	}
}}}	}
 

func main(){
        defer db.Close()
        defer db_ver.Close()
        defer db_fver.Close()
        defer db_expiry.Close()
        defer db_exp_sec.Close()
	serverMain()
}

