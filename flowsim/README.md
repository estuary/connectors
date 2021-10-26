# F L O W S I M

FlowSim is a Flow simulator suitable for building and testing connectors. 

To build FlowSim, run `make` in this directory.
To install FlowSim to your GOPATH/bin directory run `go install github.com/estuary/connectors/flowsim`

# MatSim - Materialization Simulator

The Materialization simulator is designed to be run standalone against a Docker materialization
connector image or imported directly into a materialization package and run from there to drive
the code during development.

To invoke matsim on a connector, run `flowsim matsim <test type>` See below for information on the test types.
There is more information below on how to invoke matsim from a connector during development.

## MatSim Command Options
The following command line options are shared among all of the matsim tests. 

* --image     - The docker image name for the connector (not required if invoking directly from connector)
* --network   - What docker network to run the connector on (default=host)
* --log.level - Log level for matsim output (default=info) (trace, debug, info, warn, error, fatal)
* --config    - This is the configuration for the connector (See below for format) This typically include credentials and connectivity information.
* --resource  - This is the resource configuration. (See below for format) This typically includes the table name for a materialization.

### --config / --resource format
The `--config` and `--resource` flags require yaml objects and can be supplied in 2 formats. 

The first method is the inline format where the yaml is represented on a single line such as:
```
--config="{hostname: localhost, username: myname, password: mypassword}" --resource="{table: mytable_name}"
```
The second format is a path to a yaml file with the configuration. 
```
--config=myconfig.yaml
$ cat myconfig.yaml
hostname: localhost
username: myname
password: mypassword
```

It will first attempt to parse the input as a yaml object. If it can't, it assumes it's a filename. If neither can
be parsed it will error. You can use one format for config and another for resource.

## Basic Test = basic

The basic test can be invoked using the command `flowsim matsim basic`. This test is designed to be run against
a connector that supports non-delta/upsert mode where documents are loaded and stored. This test works by
loading a bunch of documents which may or may not exist. It then stores documents, some will be new, some
will be old documents it has stored on previous passes. It then commits the documents and repeats the loop.
Once it has completed the loops it does a final load request of all the documents it has stored and verifies
that they all exist and contain the correct information.

The options for the basic test are:

* --loops - The number of load/prepare/store/commit loops to perform before verifying data.
* --start - The number of documents loaded/stored will begin at this value (default=500) and double every loop.
* --batch - The number of documents to send per request. (default=1000) When sending load/store requests 
            it is broken into batch number of documents per request.

## Delta Test - delta

The basic test can be invoked using the command `flowsim matsim delta`. The delta test does simple testing 
against adapters running in delta mode. In this mode only store/prepare/commit requests 
are sent to the connector. The options are exactly the same as the Basic  

* --loops  - The number of prepare/store/commit loops to perform before verifying data.
* --start  - The number of documents loaded/stored will begin at this value (default=500) and double every loop.
* --batch  - The number of documents to send per request. (default=1000) When sending store requests 
            it is broken into batch number of documents per request.
* --output - The documents that are stored will be sent in newline delimeted JSON format to the file specified by output. 
            If no file is specified the output will be sent to stdout. All data is sent once a commit is confirmed. 

The idea behind this test is that it will drive your connector and also stream the output. You can then
verify your data after the fact by comparing the test output with your stored data in your system outside of the test framework.

## Fence Test - fence

The basic test can be invoked using the command `flowsim matsim fence`. For connectors that support fencing 
(where the connector ensures that overlapping key ranges are not being written by multiple connector instances), 
we can test this using the fence test.

The fence test does not require any options other than the defaults. 

The test works by opening a two transactions streams with overlapping key ranges. On the first stream it then
sends a prepare/commit request and expects an error to be returned. If the connector fence is working properly it will
return an error on commit because the second stream should supersede the first.

## MatSim - Standalone

If you are developing in Go, to run matsim directly against your connector, create a main method in your connector 
and pass in a struct the fulfills the `github.com/estuary/protocols/materialize.DriverServer` interface. 
https://github.com/estuary/protocols/blob/9b8d02b54d1918f41879f708ac517a8c06495774/materialize/materialize.pb.go#L1290
(All materialization connectors need this in order to operate)

For example in the postgres driver:  https://github.com/estuary/connectors/blob/main/materialize-postgres/driver.go
```
...
import "github.com/estuary/connectors/flowsim/matsim"
...

Replace: 
func main() {
	boilerplate.RunMain(newPostgresDriver())
}

With:
func main() {
	matsim.Run(context.Background(), newPostgresDriver(), nil)
}
```

And then you can run your connector in your connector directory with:
```
go run <test type> --config="{hostname: localhost}" --resource="{table: table1}" ...any other options .
```

## MatSim - Custom Data
By default the simulator uses a simple struct of data and fills it randomly. If you would like more control over
your custom data, you can pass a function that will return test data. The test data must satisfy the 
`github.com/estuary/connectors/flowsim/testdata.TestData` interface. 

When you call the Run() function, the final option is a `func() testdata.TestData` for which you can provide your
function. If you do not provide a function, it uses the default built in stuct/data generator.
