# Project 1 

CSE 224 Winter 2022 Project 1 Starter Code

This repository contains the utilities that you'll use for project 1.  The
utilities are provided in binary form for a few common systems/architectures.

## Sort specification

This project will read, write, and sort files consisting of zero or
more records.  A record is a 100 byte binary key-value pair, consisting
of a 10-byte key and a 90-byte value.  Each key should be interpreted
as an unsigned 10-byte (80-bit) integer.  Your sort should be ascending,
meaning that the output should have the record with the smallest key first,
then the second-smallest, etc.

## Utilities

### Gensort

Gensort generates random input.  If the 'randseed' parameter is provided,
the given seed is used to ensure deterministic output.

'size' can be provided as a non-negative integer to generate that many
bytes of output. However human-readable strings can be used as well,
such as "10 mb" for 10 megabytes, "1 gb" for one gigabyte", "256 kb"
for 256 kilobytes, etc.

If the specified size is not a multiple of 100 bytes, the requested
size will be rounded up to the next multiple of 100.

Usage: bin/gensort outputfile size
  -randseed int
    	Random seed

### Showsort

Showsort shows the records in the provided file in a human-readable 
format, with the key followed by a space followed by an
abbreviated version of the value.

Usage: bin/showsort inputfile

### Valsort

Valsort scans the provided input file to check if it is sorted.

Usage: bin/valsort inputfile

## Your sort program

You are to write a sort program that reads in an input file and
produces a sorted version of the output file.

Usage: bin/sort inputfile outputfile

## Building

To build your sort program:

$ go build -o bin/sort src/sort.go

## Verifying your sort implementation

A simple way to verify the correctness of your implementation of sort
is to run the standard unix sort command on the output of 'showsort'.
For example, to generate, sort, and verify a 1 megabyte file:

```bash
$ bin/gensort example1.dat "1 mb"
No random seed provided, using current timestamp
Initializing the random seed to 1641144051385376000
Requested 1 mb (= 1048576) bytes
Increasing size to 1048600 to be divisible by 100

$ bin/sort example1.dat example1-sorted.dat
2022/01/02 09:21:16 sort.go:59: Read in 10486 records

$ bin/valsort example1-sorted.dat 
File is sorted

$ bin/showsort example1.dat | sort > example1-chk.txt
$ bin/showsort example1-sorted.dat| sort > example1-sorted-chk.txt
$ diff example1-chk.txt example1-sorted-chk.txt
```

This last 'diff' should simply return to the command prompt. If it
indicates any differences that means that there is an error
in your sort routine.

You can try this verification for different file sizes, including 0 bytes and 100 bytes
