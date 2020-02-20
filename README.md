# DBstructures
## Record structure
Every record is an array of a maximum of 30 digits. A record is considered bigger when the maximal number, that can be 'built' from the digits, is bigger.

Amount of unique numbers: 10^30
Size of record: 38 bytes
Theoretical DB size: 3.3 * 10^12 petabytes

### Example
```
[4, 3, 8, 0] -> 8430
```

## Merge Sort
Implementation of external, 2-way, natural merge sort sorting sequential files on tape drives realised in form of disk files.
The program simulates block read and write operations. Default block size is 256 records.

## Indexed sequential files
The goal of the project was to create a program to store records in form of indexed sequential files.
The program simulates block read and write operations. Default block size is 10 records. Due to the limitation of the amount of unique keys, which is 2^32, the actual maximal DB size is ~152 gigabytes.
Implemented functions: 
- [x] Add record
- [x] Search record
- [x] Delete record
- [x] Edit record
- [x] Reorganise file
