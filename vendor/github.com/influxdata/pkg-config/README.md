# Go Pkg-Config

This binary is intended as an easier way to include C code in your Go program.

This binary can be installed and Go can be told to use this binary when it invokes `pkg-config`.
If it finds a library that is known by the program, it will compile and output the location for that binary.
If it doesn't know what the program is, it will default to invoking the system `pkg-config` to obtain the compilation flags.
