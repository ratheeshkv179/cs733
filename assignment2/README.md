Ratheesh k v
153050057
M-tech 1 CSE
--------------------------------------------------------------------------------

Requirement
------------
Program is using LevelDB for storing the file contents,
So get LevelDB Packages by the following command

        go get github.com/syndtr/goleveldb/leveldb


Error Message Meaning
-----------------------

ERR_VERSION : Indicate a file with specified version is not available.
                i.e File is present but the version number mentioned was wrong
                        it occur in 'cas' command

ERR_FILE_NOT_FOUND : File with specified name is not present

ERR_CMD_ERR : Means any mistake in commands like 
        1) write : error may be due to,
               (a) Number of fields in 'write' command are wrong
               (b) Other than 'write', 'read', 'cas', 'delete' commands are used
               (c) The file name specified is exceeds 256 bytes
             

ERR_INTERNAL : Any other error, other than mentioned above
          (a) The number of byte specified is not a number
          (b) The expiry time specified is not a number
          (c) Any problem with reading/writiing to socket
          (d) Any other formating error, mismatch between number
                of byte specified and number of bytes in the 'content bytes'
 

Following are successfully tested while the server is continuously running
--------------------------------------------------------------------------

1) Write to file
2) Read From file
3) Delete a file
4) Compare and swap a file
5) Version management is successfull

